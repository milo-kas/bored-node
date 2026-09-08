use crate::core::{protocol::StarredMetadata, queue::ClipboardItem};
use rusqlite::{Connection, params};
use std::path::{Path, PathBuf};
use tokio::sync::{mpsc, oneshot};

/// Messages sent to the dedicated database thread.
pub enum DbCommand {
    Insert {
        item: ClipboardItem,
        responder: oneshot::Sender<Result<(), String>>,
    },
    Delete {
        id: u128,
        responder: oneshot::Sender<Result<(), rusqlite::Error>>,
    },
    GetPreviewPage {
        limit: usize,
        offset: usize,
        responder: oneshot::Sender<Result<Vec<StarredMetadata>, rusqlite::Error>>,
    },
    GetFullText {
        id: u128,
        responder: oneshot::Sender<Result<String, rusqlite::Error>>,
    },
}

pub struct Database {
    conn: Connection,
}

impl Database {
    pub fn init(path: &Path) -> Result<Self, rusqlite::Error> {
        let conn = Connection::open(path)?;
        conn.execute_batch("PRAGMA auto_vacuum = FULL;")?;
        conn.execute(
            "CREATE TABLE IF NOT EXISTS starred (
                id TEXT PRIMARY KEY,
                text TEXT NOT NULL,
                size_bytes INTEGER NOT NULL,
                from_peer TEXT,
                created_at INTEGER NOT NULL
            )",
            [],
        )?;
        Ok(Self { conn })
    }

    pub fn insert(&self, item: &ClipboardItem) -> Result<(), String> {
        let count: i64 = self
            .conn
            .query_row("SELECT COUNT(*) FROM starred", [], |row| row.get(0))
            .map_err(|e| e.to_string())?;

        if count >= 999 {
            return Err("starred item limit reached (999)".to_string());
        }

        let created_at_ms = (item.id / 1_000_000) as i64;

        self.conn
            .execute(
                "INSERT INTO starred (id, text, size_bytes, from_peer, created_at) VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    item.id.to_string(),
                    item.text,
                    item.size_bytes as i64,
                    item.from,
                    created_at_ms
                ],
            )
            .map_err(|e| e.to_string())?;

        Ok(())
    }

    pub fn delete(&self, id: u128) -> Result<(), rusqlite::Error> {
        self.conn
            .execute("DELETE FROM starred WHERE id = ?1", params![id.to_string()])?;
        Ok(())
    }

    pub fn get_preview_page(
        &self,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<StarredMetadata>, rusqlite::Error> {
        let mut stmt = self.conn.prepare(
            "SELECT id, substr(text, 1, 200), size_bytes, from_peer
             FROM starred
             ORDER BY created_at DESC
             LIMIT ? OFFSET ?",
        )?;

        let rows = stmt.query_map(params![limit as i64, offset as i64], |row| {
            let id_str: String = row.get(0)?;
            let size_bytes_i64: i64 = row.get(2)?;
            Ok(StarredMetadata {
                id: id_str
                    .parse::<u128>()
                    .map_err(|_| rusqlite::Error::IntegralValueOutOfRange(0, 0))?,
                preview: row.get(1)?,
                size_bytes: size_bytes_i64 as usize,
                from: row.get(3)?,
            })
        })?;

        rows.collect()
    }

    pub fn get_full_text(&self, id: u128) -> Result<String, rusqlite::Error> {
        self.conn.query_row(
            "SELECT text FROM starred WHERE id = ?1",
            params![id.to_string()],
            |row| row.get(0),
        )
    }
}

/// Spawns a dedicated OS thread owning the database connection.
pub fn spawn_db_actor(db_path: PathBuf) -> mpsc::Sender<DbCommand> {
    let (db_tx, mut db_rx) = mpsc::channel::<DbCommand>(64);
    std::thread::spawn(move || {
        let db = Database::init(&db_path).expect("Failed to init DB");
        while let Some(command) = db_rx.blocking_recv() {
            match command {
                DbCommand::Insert { item, responder } => {
                    let _ = responder.send(db.insert(&item));
                }
                DbCommand::Delete { id, responder } => {
                    let _ = responder.send(db.delete(id));
                }
                DbCommand::GetPreviewPage {
                    limit,
                    offset,
                    responder,
                } => {
                    let _ = responder.send(db.get_preview_page(limit, offset));
                }
                DbCommand::GetFullText { id, responder } => {
                    let _ = responder.send(db.get_full_text(id));
                }
            }
        }
    });
    db_tx
}
