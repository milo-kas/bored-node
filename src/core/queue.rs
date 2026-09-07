use std::{
    collections::VecDeque,
    fmt,
    time::{SystemTime, UNIX_EPOCH},
};

pub const MAX_QUEUE_ITEMS: usize = 20;
pub const MAX_QUEUE_BYTES: usize = 400_000_000;

#[derive(Debug, Clone)]
pub struct ClipboardItem {
    pub id: u128,
    pub text: String,
    pub size_bytes: usize,
    pub from: Option<String>,
}

impl ClipboardItem {
    pub fn new(text: String) -> Self {
        Self::new_with_source(text, None)
    }

    pub fn new_with_source(text: String, from: Option<String>) -> Self {
        let id = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();

        let size_bytes = text.len();
        Self {
            id,
            text,
            size_bytes,
            from,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueError {
    QueueFull,
}

impl fmt::Display for QueueError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::QueueFull => write!(f, "queue full"),
        }
    }
}

impl std::error::Error for QueueError {}

#[derive(Debug, Default, Clone)]
pub struct ClipboardQueue {
    pub pending: VecDeque<ClipboardItem>,
    pub starred: Vec<ClipboardItem>,
    pub current_bytes: usize,
}

impl ClipboardQueue {
    pub fn push(&mut self, text: String) -> Result<(), QueueError> {
        self.push_with_source(text, None)
    }

    pub fn push_with_source(
        &mut self,
        text: String,
        from: Option<String>,
    ) -> Result<(), QueueError> {
        let new_size = text.len();

        if self.pending.len() >= MAX_QUEUE_ITEMS {
            return Err(QueueError::QueueFull);
        }

        if self.current_bytes + new_size > MAX_QUEUE_BYTES {
            return Err(QueueError::QueueFull);
        }

        let item = ClipboardItem::new_with_source(text, from);
        self.current_bytes += item.size_bytes;
        self.pending.push_back(item);
        Ok(())
    }

    pub fn peek_current(&self) -> Option<&ClipboardItem> {
        self.pending.front()
    }

    pub fn next(&mut self) -> Option<ClipboardItem> {
        let item = self.pending.pop_front();
        if let Some(ref current) = item {
            self.current_bytes = self.current_bytes.saturating_sub(current.size_bytes);
        }
        item
    }

    pub fn star_current(&mut self) -> Option<ClipboardItem> {
        let item = self.pending.pop_front();
        if let Some(ref current) = item {
            self.current_bytes = self.current_bytes.saturating_sub(current.size_bytes);
            self.starred.push(current.clone());
        }
        item
    }

    pub fn list_all(&self) -> impl Iterator<Item = &ClipboardItem> {
        self.pending.iter()
    }

    pub fn list_starred(&self) -> impl Iterator<Item = &ClipboardItem> {
        self.starred.iter()
    }
}
