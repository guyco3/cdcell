// Copyright 2026 Guy Cohen. Apache-2.0 license.

//! Pipeline scenarios against a fake celld, in memory with paused time:
//! streaming, and recovery when celld compacted or GC'd the committed
//! point while cdcell was down (design §6). Each runs in well under a
//! second; the e2e equivalents (`m5-l1-resync`, `m5-epoch-gc`) take minutes.

mod common;

use common::Harness;

const DDL: &str = "CREATE TABLE orders(id INTEGER PRIMARY KEY, status TEXT);";
const ORACLE: &str = "SELECT id, status FROM orders";

async fn orders(h: &mut Harness, cell: &str, from: i64, to: i64) {
    for i in from..=to {
        h.celld
            .exec(cell, &format!("INSERT INTO orders VALUES ({i}, 'new')"))
            .await;
    }
}

fn assert_type1(h: &Harness, cell: &str) {
    let got = h.type1("orders", cell, &["id", "status"]);
    let want = h.celld.rows(cell, ORACLE);
    assert_eq!(
        got, want,
        "type-1 of the change log != the Durable Object's rows"
    );
}

#[tokio::test(start_paused = true)]
async fn streams_inserts_updates_deletes() {
    let mut h = Harness::new().await;
    let cell = h.celld.create("acme", DDL).await;
    orders(&mut h, &cell, 1, 5).await;
    h.celld
        .exec(&cell, "UPDATE orders SET status='paid' WHERE id=2")
        .await;
    h.celld.exec(&cell, "DELETE FROM orders WHERE id=3").await;
    h.deliver().await;
    h.run(5).await;
    assert_type1(&h, &cell);
}

/// m5-l1-resync, idle variant: committed at TXID ~6; while cdcell is down
/// the cell changes, celld folds its history into an L1 from TXID 1 and
/// deletes the L0s, and the cell then goes idle (no later write). The L1's
/// notification is the only trace: the restart must resync from it (rows +
/// owed deletes) instead of keeping the stale state.
#[tokio::test(start_paused = true)]
async fn resync_when_l1_fold_passes_committed_point() {
    let mut h = Harness::new().await;
    let cell = h.celld.create("acme", DDL).await;
    orders(&mut h, &cell, 1, 5).await;
    h.deliver().await;
    h.run(5).await;
    h.stop();
    h.celld
        .exec(&cell, "DELETE FROM orders WHERE id IN (1, 4)")
        .await;
    h.celld
        .exec(&cell, "UPDATE orders SET status='paid' WHERE id=2")
        .await;
    orders(&mut h, &cell, 6, 9).await;
    h.celld.fold_l1(&cell).await;
    h.restart().await;
    h.deliver().await;
    h.run(40).await;
    assert_type1(&h, &cell);
}

/// m5-epoch-gc: committed in epoch 1; while cdcell is down the cell moves
/// to epochs 2 and 3 (each opens with a TXID-1 image) and celld's epoch GC
/// deletes epochs 1 and 2. The restart must resync from the owner's epoch.
#[tokio::test(start_paused = true)]
async fn resync_when_committed_epoch_is_gcd() {
    let mut h = Harness::new().await;
    let cell = h.celld.create("acme", DDL).await;
    orders(&mut h, &cell, 1, 5).await;
    h.deliver().await;
    h.run(5).await;
    h.stop();
    h.celld.activate(&cell, 2).await;
    h.celld.exec(&cell, "DELETE FROM orders WHERE id=1").await;
    orders(&mut h, &cell, 6, 7).await;
    h.celld.activate(&cell, 3).await;
    h.celld
        .exec(&cell, "UPDATE orders SET status='paid' WHERE id=6")
        .await;
    h.celld.gc_epoch(&cell, 1).await;
    h.celld.gc_epoch(&cell, 2).await;
    h.restart().await;
    h.deliver().await;
    h.run(40).await;
    assert_type1(&h, &cell);
}

/// Baseline: streaming through a paged activation (the marker cuts the
/// previous epoch; no TXID-1 image in the new epoch).
#[tokio::test(start_paused = true)]
async fn streams_through_paged_activation() {
    let mut h = Harness::new().await;
    let cell = h.celld.create("acme", DDL).await;
    orders(&mut h, &cell, 1, 3).await;
    h.deliver().await;
    h.run(5).await;
    h.celld.activate_paged(&cell, 2).await;
    h.celld.exec(&cell, "DELETE FROM orders WHERE id=2").await;
    orders(&mut h, &cell, 4, 5).await;
    h.deliver().await;
    h.run(5).await;
    assert_type1(&h, &cell);
}

/// cdcell first sees a cell after it moved to a paged epoch (its first start
/// against a running deployment): the older epoch's objects can no longer be
/// confirmed by own.json and the owner's epoch has no TXID-1 image. The
/// bootstrap must follow the restore chain.
#[tokio::test(start_paused = true)]
async fn bootstraps_a_cell_first_seen_in_a_paged_epoch() {
    let mut h = Harness::new().await;
    let cell = h.celld.create("acme", DDL).await;
    orders(&mut h, &cell, 1, 3).await;
    h.celld.exec(&cell, "DELETE FROM orders WHERE id=1").await;
    h.celld.activate_paged(&cell, 2).await;
    orders(&mut h, &cell, 4, 5).await;
    h.deliver().await;
    h.run(30).await;
    assert_type1(&h, &cell);
    let ops: Vec<String> = h
        .sink
        .rows("orders")
        .iter()
        .filter_map(|r| match r.get("__op") {
            Some(cdcell::iceberg_out::OutValue::Str(o)) => Some(o.clone()),
            _ => None,
        })
        .collect();
    assert!(
        ops.iter().all(|o| o == "r"),
        "initial state is emitted as `r`: {ops:?}"
    );
}

/// Restart with the committed point inside a paged epoch: the base is in
/// the epoch below. The rebuild must follow the chain, not wait forever for
/// a TXID-1 object in the committed epoch.
#[tokio::test(start_paused = true)]
async fn restart_inside_paged_epoch() {
    let mut h = Harness::new().await;
    let cell = h.celld.create("acme", DDL).await;
    orders(&mut h, &cell, 1, 3).await;
    h.deliver().await;
    h.run(5).await;
    h.celld.activate_paged(&cell, 2).await;
    orders(&mut h, &cell, 4, 5).await;
    h.deliver().await;
    h.run(5).await;
    assert!(
        h.sink.rows("orders").iter().any(|r| matches!(
            r.get("__epoch"),
            Some(cdcell::iceberg_out::OutValue::Long(2))
        )),
        "committed inside the paged epoch before the restart"
    );
    h.stop();
    h.celld
        .exec(&cell, "UPDATE orders SET status='paid' WHERE id=4")
        .await;
    h.celld.exec(&cell, "DELETE FROM orders WHERE id=1").await;
    h.restart().await;
    h.deliver().await;
    h.run(40).await;
    assert_type1(&h, &cell);
}

/// The committed epoch is GC'd and the owner's epoch is paged (its base is
/// an intermediate epoch's TXID-1 image): resync along the chain.
#[tokio::test(start_paused = true)]
async fn resync_when_owner_epoch_is_paged_and_committed_epoch_gcd() {
    let mut h = Harness::new().await;
    let cell = h.celld.create("acme", DDL).await;
    orders(&mut h, &cell, 1, 4).await;
    h.deliver().await;
    h.run(5).await;
    h.stop();
    h.celld.activate(&cell, 2).await;
    h.celld.exec(&cell, "DELETE FROM orders WHERE id=2").await;
    h.celld.activate_paged(&cell, 3).await;
    orders(&mut h, &cell, 5, 6).await;
    h.celld
        .exec(&cell, "UPDATE orders SET status='paid' WHERE id=5")
        .await;
    h.celld.gc_epoch(&cell, 1).await;
    h.restart().await;
    h.deliver().await;
    h.run(40).await;
    assert_type1(&h, &cell);
}
