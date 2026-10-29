//! Enterprise ingestion and Refiner connector delivery. Gail owns AI providers,
//! OctoBot owns trading, and Continuum owns infrastructure. No arbitrary URL,
//! shell command, retry loop or client-provided approval can cross this boundary.
use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::HeaderMap,
    routing::{get, post},
};
use nm_workflow_core::{
    ApiError, App, Identity, Result, SourceRef, Value, digest, identifier, json, now, require,
};
use serde::{Deserialize, Serialize};
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Change {
    pub id: String,
    pub source: SourceRef,
    pub data: Value,
}
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Batch {
    pub source_id: String,
    pub expected_checkpoint: Option<String>,
    pub checkpoint: String,
    pub changes: Vec<Change>,
}
pub fn validate_batch(batch: &Batch, actor: &Identity, clock: i64) -> Result<()> {
    require(
        identifier(&batch.source_id)
            && identifier(&batch.checkpoint)
            && !batch.changes.is_empty()
            && batch.changes.len() <= 100
            && batch.expected_checkpoint.as_ref() != Some(&batch.checkpoint),
        "batch bounds",
    )?;
    let mut ids = std::collections::BTreeSet::new();
    for change in &batch.changes {
        change.source.validate(&actor.tenant, clock)?;
        require(
            identifier(&change.id)
                && ids.insert(&change.id)
                && change.source.source_id == batch.source_id
                && (!change.source.deleted || change.data.is_null()),
            "source or tombstone",
        )?;
    }
    Ok(())
}
async fn ingest(
    State(app): State<App>,
    headers: HeaderMap,
    Json(batch): Json<Batch>,
) -> Result<Json<Value>> {
    let actor = app.authorize(&headers, "control").await?;
    validate_batch(&batch, &actor, now())?;
    let mut db = app.store.pool.get().await?;
    let tx = db.transaction().await?;
    // One source is serialized; unrelated sources use separate database locks.
    // The checkpoint and all change records commit atomically or not at all.
    tx.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
        &[&format!("{}:{}", actor.tenant, batch.source_id)],
    )
    .await?;
    let existing=tx.query_opt("SELECT payload FROM workflow_records WHERE tenant=$1 AND kind='checkpoint' AND id=$2 FOR UPDATE",&[&actor.tenant,&batch.source_id]).await?;
    let current = existing.as_ref().map(|r| r.get::<_, Value>(0));
    let fingerprint = digest(&batch);
    if current.as_ref().and_then(|v| v["checkpoint"].as_str()) == Some(&batch.checkpoint) {
        if current.as_ref().and_then(|v| v["batch_digest"].as_str()) == Some(&fingerprint) {
            return Ok(Json(json!({"checkpoint":batch.checkpoint,"replayed":true})));
        }
        return Err(ApiError::conflict());
    }
    if current.as_ref().and_then(|v| v["checkpoint"].as_str())
        != batch.expected_checkpoint.as_deref()
    {
        return Err(ApiError::conflict());
    }
    for change in &batch.changes {
        let value = json!({"change":change,"ingested_at":now(),"actor":actor.principal});
        let inserted=tx.execute("INSERT INTO workflow_records(tenant,kind,id,owner,payload) VALUES($1,'change',$2,$3,$4) ON CONFLICT DO NOTHING",
            &[&actor.tenant,&change.id,&actor.principal,&value]).await?;
        if inserted == 0 {
            let old:Value=tx.query_one("SELECT payload FROM workflow_records WHERE tenant=$1 AND kind='change' AND id=$2",&[&actor.tenant,&change.id]).await?.get(0);
            if old["change"] != json!(change) {
                return Err(ApiError::conflict());
            }
            continue;
        }
        let source = json!(change.source);
        tx.execute("INSERT INTO workflow_records(tenant,kind,id,owner,payload) VALUES($1,'source',$2,$3,$4) ON CONFLICT(tenant,kind,id) DO UPDATE SET payload=EXCLUDED.payload",
            &[&actor.tenant,&change.source.key(),&actor.principal,&source]).await?;
    }
    let value =
        json!({"checkpoint":batch.checkpoint,"batch_digest":fingerprint,"updated_at":now()});
    tx.execute("INSERT INTO workflow_records(tenant,kind,id,owner,payload) VALUES($1,'checkpoint',$2,$3,$4) ON CONFLICT(tenant,kind,id) DO UPDATE SET payload=EXCLUDED.payload",
        &[&actor.tenant,&batch.source_id,&actor.principal,&value]).await?;
    tx.commit().await?;
    Ok(Json(
        json!({"checkpoint":batch.checkpoint,"replayed":false}),
    ))
}
#[derive(Deserialize)]
struct Cursor {
    #[serde(default)]
    after: i64,
    #[serde(default = "context_purpose")]
    purpose: String,
}
fn context_purpose() -> String {
    "context".into()
}
async fn changes(
    State(app): State<App>,
    headers: HeaderMap,
    Query(cursor): Query<Cursor>,
) -> Result<Json<Value>> {
    let actor = app.authorize(&headers, "observe").await?;
    require(
        cursor.after >= 0 && identifier(&cursor.purpose),
        "cursor and purpose",
    )?;
    let rows = app
        .store
        .list(&actor.tenant, "change", cursor.after, 100)
        .await?;
    let next = rows
        .last()
        .and_then(|v| v["_sequence"].as_i64())
        .unwrap_or(cursor.after);
    let mut permitted = Vec::new();
    for record in rows {
        let source: SourceRef = serde_json::from_value(record["change"]["source"].clone())
            .map_err(|_| ApiError::unavailable())?;
        let current: SourceRef = serde_json::from_value(
            app.store
                .get(&actor.tenant, "source", &source.key())
                .await?,
        )
        .map_err(|_| ApiError::unavailable())?;
        // Denial events are delivered to their original readers to invalidate
        // caches; their data is null. Historical payloads obey current ACLs.
        if current.permits(&actor, &cursor.purpose, now())
            || (current.deleted && source.readers.contains(&actor.principal))
        {
            permitted.push(if current.deleted {json!({"change":{"id":record["change"]["id"],"source":current,"data":null},"_sequence":record["_sequence"]})} else {record});
        }
    }
    Ok(Json(json!({"changes":permitted,"next_cursor":next})))
}
#[derive(Deserialize)]
struct Purpose {
    purpose: String,
}
async fn source(
    State(app): State<App>,
    headers: HeaderMap,
    Path((source, object)): Path<(String, String)>,
    Query(purpose): Query<Purpose>,
) -> Result<Json<Value>> {
    let actor = app.authorize(&headers, "observe").await?;
    require(
        identifier(&source) && identifier(&object) && identifier(&purpose.purpose),
        "source identity",
    )?;
    let value = app
        .store
        .get(&actor.tenant, "source", &digest(&(&source, &object)))
        .await?;
    let reference: SourceRef =
        serde_json::from_value(value.clone()).map_err(|_| ApiError::unavailable())?;
    let deletion_receipt = purpose.purpose == "deletion"
        && reference.deleted
        && reference.access_expires_at > now()
        && reference.readers.contains(&actor.principal);
    if !reference.permits(&actor, &purpose.purpose, now()) && !deletion_receipt {
        return Err(ApiError::forbidden());
    }
    Ok(Json(value))
}
/// The approved operation is compared as a complete JSON value with Conductor's
/// stored plan. Approval cannot be transferred to changed arguments or targets.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Operation {
    pub version: u32,
    pub idempotency_key: String,
    pub work_id: String,
    pub connector: String,
    pub server: String,
    pub tool: String,
    pub arguments: Value,
    pub source: SourceRef,
}
pub fn approval_matches(work: &Value, op: &Operation) -> bool {
    work["execution_approved"] == true
        && !matches!(
            work["status"].as_str(),
            Some("rejected" | "cancelled" | "completed" | "failed")
        )
        && work["plan"]["relay_operation"] == json!(op)
}
async fn execute(
    State(app): State<App>,
    headers: HeaderMap,
    Json(op): Json<Operation>,
) -> Result<Json<Value>> {
    let actor = app.authorize(&headers, "control").await?;
    require(
        op.version == 1
            && identifier(&op.idempotency_key)
            && identifier(&op.work_id)
            && op.connector == "refiner_mcp"
            && identifier(&op.server)
            && identifier(&op.tool)
            && op.arguments.is_object(),
        "operation contract",
    )?;
    op.source.validate(&actor.tenant, now())?;
    let current: SourceRef = serde_json::from_value(
        app.store
            .get(&actor.tenant, "source", &op.source.key())
            .await?,
    )
    .map_err(|_| ApiError::unavailable())?;
    require(
        current.version == op.source.version && current.permits(&actor, "execute", now()),
        "action source access",
    )?;
    let conductor = App::upstream(
        "conductor",
        "http://conductor.conductor.svc.cluster.local:8091",
    );
    let work = app
        .get(
            &format!("{conductor}/api/v1/work-items/{}", op.work_id),
            &App::upstream_token("conductor"),
        )
        .await?;
    let work = work.get("work_item").unwrap_or(&work);
    if !approval_matches(work, &op) {
        return Err(ApiError::forbidden());
    }
    let fingerprint = digest(&op);
    let db = app.store.pool.get().await?;
    let claim = json!({"operation":op,"operation_digest":fingerprint,"actor":actor.principal,
        "state":"reconciliation_required","reason":"dispatch_may_have_started","claimed_at":now()});
    // Claim before the network call. Crash or cancellation leaves an uncertain
    // durable receipt, so retrying the request cannot repeat a remote write.
    let inserted=db.execute("INSERT INTO workflow_records(tenant,kind,id,owner,payload) VALUES($1,'receipt',$2,$3,$4) ON CONFLICT DO NOTHING",
        &[&actor.tenant,&op.idempotency_key,&actor.principal,&claim]).await?;
    if inserted == 0 {
        let old: Value = db
            .query_one(
                "SELECT payload FROM workflow_records WHERE tenant=$1 AND kind='receipt' AND id=$2",
                &[&actor.tenant, &op.idempotency_key],
            )
            .await?
            .get(0);
        if old["operation_digest"] != fingerprint || old["actor"] != actor.principal {
            return Err(ApiError::conflict());
        }
        return Ok(Json(old));
    }
    let refiner = App::upstream("refiner", "http://refiner.refiner.svc.cluster.local:5001");
    let result = app
        .post(
            &format!("{refiner}/api/mcp/servers/{}/call", op.server),
            &actor.bearer,
            &json!({"tool":op.tool,"arguments":op.arguments}),
        )
        .await;
    let mut receipt = claim;
    match result {
        Ok(value) => {
            // MCP protocol success may contain tool-level failure. Retain the
            // receipt without claiming completed business effects.
            receipt["state"] = json!(if value.pointer("/result/isError") == Some(&json!(false)) {
                "completed"
            } else {
                "reconciliation_required"
            });
            receipt["result"] = value;
        }
        Err(_) => receipt["reason"] = json!("remote_result_unknown"),
    }
    receipt["updated_at"] = json!(now());
    db.execute("UPDATE workflow_records SET payload=$4 WHERE tenant=$1 AND kind='receipt' AND id=$2 AND owner=$3",
        &[&actor.tenant,&op.idempotency_key,&actor.principal,&receipt]).await?;
    Ok(Json(receipt))
}
async fn receipt(
    State(app): State<App>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Json<Value>> {
    let actor = app.authorize(&headers, "observe").await?;
    let value = app.store.get(&actor.tenant, "receipt", &id).await?;
    if value["actor"] != actor.principal {
        return Err(ApiError::forbidden());
    }
    if value["purged"] != true {
        let reference: SourceRef = serde_json::from_value(value["operation"]["source"].clone())
            .map_err(|_| ApiError::unavailable())?;
        let current: SourceRef = serde_json::from_value(
            app.store
                .get(&actor.tenant, "source", &reference.key())
                .await?,
        )
        .map_err(|_| ApiError::unavailable())?;
        if !current.permits(&actor, "execute", now()) {
            return Err(ApiError::forbidden());
        }
    }
    Ok(Json(value))
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReadResource {
    server: String,
    uri: String,
}
async fn read_refiner(
    State(app): State<App>,
    headers: HeaderMap,
    Json(input): Json<ReadResource>,
) -> Result<Json<Value>> {
    let actor = app.authorize(&headers, "use").await?;
    require(
        identifier(&input.server) && !input.uri.is_empty() && input.uri.len() <= 2048,
        "resource",
    )?;
    let refiner = App::upstream("refiner", "http://refiner.refiner.svc.cluster.local:5001");
    Ok(Json(
        app.post(
            &format!("{refiner}/api/mcp/servers/{}/resource", input.server),
            &actor.bearer,
            &json!({"uri":input.uri}),
        )
        .await?,
    ))
}
async fn capabilities(State(app): State<App>, headers: HeaderMap) -> Result<Json<Value>> {
    app.authorize(&headers, "observe").await?;
    Ok(Json(
        json!({"ingestion":"transactional_checkpoint","connector":"refiner_mcp","action_approval":"conductor_exact_plan","uncertain_actions":"reconciliation_required","automatic_write_retries":false,"exactly_once_external_effects":false}),
    ))
}
pub fn routes() -> Router<App> {
    Router::new()
        .route("/v1/ingest", post(ingest))
        .route("/v1/changes", get(changes))
        .route("/v1/sources/{source}/{object}", get(source))
        .route("/v1/actions", post(execute))
        .route("/v1/receipts/{id}", get(receipt))
        .route("/v1/connectors/refiner/read", post(read_refiner))
        .route("/v1/capabilities", get(capabilities))
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn altered_operation_and_client_approval_are_rejected() {
        let source = SourceRef {
            source_id: "erp".into(),
            object_id: "o1".into(),
            version: "v1".into(),
            tenant: "t".into(),
            readers: vec!["alice".into()],
            purposes: vec!["execute".into()],
            observed_at: 1,
            access_expires_at: 200,
            deleted: false,
            training_permitted: false,
        };
        let mut op = Operation {
            version: 1,
            idempotency_key: "k1".into(),
            work_id: "w1".into(),
            connector: "refiner_mcp".into(),
            server: "erp".into(),
            tool: "update".into(),
            arguments: json!({"quantity":1}),
            source,
        };
        let work =
            json!({"execution_approved":true,"status":"approved","plan":{"relay_operation":op}});
        assert!(approval_matches(&work, &op));
        op.arguments = json!({"quantity":2});
        assert!(!approval_matches(&work, &op));
        assert!(!approval_matches(&json!({"execution_approved":true}), &op));
    }
}
