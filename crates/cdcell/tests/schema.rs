// Copyright 2026 Guy Cohen. Apache-2.0 license.

//! Schema changes inside a Durable Object, end to end through the pipeline.

mod common;

use common::Harness;

const DDL: &str = "CREATE TABLE orders(id INTEGER PRIMARY KEY, status TEXT);";

/// DROP TABLE: every row of the table is gone, so type 1 must be empty.
#[tokio::test(start_paused = true)]
async fn dropped_table_rows_are_deleted() {
    let mut h = Harness::new().await;
    let cell = h.celld.create("acme", DDL).await;
    for i in 1..=3 {
        h.celld
            .exec(&cell, &format!("INSERT INTO orders VALUES ({i}, 'new')"))
            .await;
    }
    h.deliver().await;
    h.run(3).await;
    h.celld.exec(&cell, "DROP TABLE orders").await;
    h.deliver().await;
    h.run(3).await;
    assert!(
        h.type1("orders", &cell, &["id"]).is_empty(),
        "rows of a dropped table must be deleted"
    );
}

/// RENAME: the old name's rows are deleted, the new name has them.
#[tokio::test(start_paused = true)]
async fn renamed_table_moves_rows() {
    let mut h = Harness::new().await;
    let cell = h.celld.create("acme", DDL).await;
    for i in 1..=3 {
        h.celld
            .exec(&cell, &format!("INSERT INTO orders VALUES ({i}, 'new')"))
            .await;
    }
    h.deliver().await;
    h.run(3).await;
    h.celld
        .exec(&cell, "ALTER TABLE orders RENAME TO orders_v2")
        .await;
    h.deliver().await;
    h.run(3).await;
    assert!(h.type1("orders", &cell, &["id"]).is_empty());
    assert_eq!(
        h.type1("orders_v2", &cell, &["id", "status"]),
        h.celld.rows(&cell, "SELECT id, status FROM orders_v2")
    );
}
