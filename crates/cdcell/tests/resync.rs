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
