use bored_node::core::db::{DbCommand, spawn_db_actor};
use bored_node::core::queue::ClipboardItem;
use std::env::temp_dir;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::oneshot;

#[tokio::test]
async fn test_db_actor_lifecycle() {
    // Setup temporary database path
    let mut db_path = temp_dir();
    db_path.push(format!(
        "test_bored_node_{}.db",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis()
    ));

    // Spawn the actor
    let db_tx = spawn_db_actor(db_path.clone());

    // Test Insert
    let item = ClipboardItem::new("Actor Test String".to_string());
    let item_id = item.id;
    let (insert_tx, insert_rx) = oneshot::channel();

    db_tx
        .send(DbCommand::Insert {
            item,
            responder: insert_tx,
        })
        .await
        .unwrap();
    let insert_res = insert_rx.await.expect("Channel dropped");
    assert!(insert_res.is_ok(), "Insert failed");

    // Test GetFullText
    let (get_tx, get_rx) = oneshot::channel();
    db_tx
        .send(DbCommand::GetFullText {
            id: item_id,
            responder: get_tx,
        })
        .await
        .unwrap();

    let text_res = get_rx.await.expect("Channel dropped").expect("DB Error");
    assert_eq!(text_res, "Actor Test String");

    // Test Delete
    let (del_tx, del_rx) = oneshot::channel();
    db_tx
        .send(DbCommand::Delete {
            id: item_id,
            responder: del_tx,
        })
        .await
        .unwrap();
    let del_res = del_rx.await.expect("Channel dropped");
    assert!(del_res.is_ok());

    // Cleanup
    let _ = std::fs::remove_file(db_path);
}

#[tokio::test]
async fn test_db_actor_max_capacity() {
    let mut db_path = temp_dir();
    db_path.push(format!(
        "test_bored_node_capacity_{}.db",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis()
    ));

    let db_tx = spawn_db_actor(db_path.clone());

    // Fill the database to the max limit (999 items)
    for i in 0..999 {
        let item = ClipboardItem::new(format!("Capacity test item {}", i));
        let (insert_tx, insert_rx) = oneshot::channel();

        db_tx
            .send(DbCommand::Insert {
                item,
                responder: insert_tx,
            })
            .await
            .unwrap();

        let insert_res = insert_rx.await.expect("Channel dropped");
        assert!(
            insert_res.is_ok(),
            "Failed to insert item {} within the 999 capacity limit",
            i
        );
    }

    // Attempt to insert the 1000th item
    let overflow_item = ClipboardItem::new("This item should be rejected".to_string());
    let (overflow_tx, overflow_rx) = oneshot::channel();

    db_tx
        .send(DbCommand::Insert {
            item: overflow_item,
            responder: overflow_tx,
        })
        .await
        .unwrap();

    let overflow_res = overflow_rx.await.expect("Channel dropped");

    // Should fail with an Err return because the limit was reached
    assert!(
        overflow_res.is_err(),
        "The 1,000th insert succeeded, but it should have been blocked by the limit!"
    );

    // Cleanup
    let _ = std::fs::remove_file(db_path);
}
