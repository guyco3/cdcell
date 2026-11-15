// Copyright 2026 Guy Cohen. Apache-2.0 license.

//! Fleet-mode scenarios: bundles confirmed by the node lease, zombie
//! copies, and recovery when the owner's epoch exists only in bundles.

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
async fn streams_bundles() {
    let mut h = Harness::new().await;
    h.celld.fleet().await;
    let cell = h.celld.create("acme", DDL).await;
    orders(&mut h, &cell, 1, 4).await;
    h.celld.exec(&cell, "DELETE FROM orders WHERE id=2").await;
    h.celld.flush().await;
    h.deliver().await;
    h.run(5).await;
    assert_type1(&h, &cell);
}

/// A sealed session's bundle rows for TXIDs 5..12 are suspects; recovery
/// folded the session into the cell's prefix as one confirmed object 5-12.
/// The suspect (E,5,5) must not shadow it.
#[tokio::test(start_paused = true)]
async fn zombie_copy_does_not_shadow_confirmed_history() {
    let mut h = Harness::new().await;
    h.celld.fleet().await;
    let cell = h.celld.create("acme", DDL).await;
    orders(&mut h, &cell, 1, 3).await; // TXIDs 2..4
    h.celld.flush().await;
    h.deliver().await;
    h.run(3).await;
    orders(&mut h, &cell, 4, 11).await; // TXIDs 5..12
    h.celld.flush().await;
    h.celld.seal().await;
    h.deliver_matching(".ltxb").await;
    h.run(2).await; // the bundle rows become suspects
    h.celld.drain_merged(&cell, 5, 12).await;
    h.deliver().await;
    h.run(5).await;
    assert_type1(&h, &cell);
}

/// Committed in epoch 1; while cdcell is down the cell is folded and
/// reactivated in epoch 2, which exists only in a bundle (no per-cell
/// objects yet), and celld GCs epoch 1 and the old bundles.
#[tokio::test(start_paused = true)]
async fn resync_when_owner_epoch_is_only_in_bundles() {
    let mut h = Harness::new().await;
    h.celld.fleet().await;
    let cell = h.celld.create("acme", DDL).await;
    orders(&mut h, &cell, 1, 3).await;
    h.celld.flush().await;
    h.deliver().await;
    h.run(5).await;
    h.stop();
    h.celld.fold_l1(&cell).await;
    h.celld.gc_bundles().await;
    h.celld.activate(&cell, 2).await;
    h.celld.exec(&cell, "DELETE FROM orders WHERE id=1").await;
    orders(&mut h, &cell, 4, 5).await;
    h.celld.flush().await;
    h.celld.gc_epoch(&cell, 1).await;
    h.restart().await;
    h.deliver().await;
    h.run(40).await;
    assert_type1(&h, &cell);
}
