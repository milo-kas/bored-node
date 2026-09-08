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
        }
        item
    }

    pub fn list_all(&self) -> impl Iterator<Item = &ClipboardItem> {
        self.pending.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_push_respects_count_limit() {
        let mut queue = ClipboardQueue::default();

        for i in 0..MAX_QUEUE_ITEMS {
            queue.push(format!("item-{i}")).unwrap();
        }

        assert_eq!(queue.pending.len(), MAX_QUEUE_ITEMS);
        assert_eq!(
            queue.push("overflow".to_string()),
            Err(QueueError::QueueFull)
        );
        assert_eq!(queue.pending.len(), MAX_QUEUE_ITEMS);
    }

    #[test]
    fn test_push_respects_memory_limit() {
        let mut queue = ClipboardQueue::default();
        let chunk_size = 200_000_000; // 200MB chunks
        let chunk = "A".repeat(chunk_size);

        // Push 2 chunks (count = 2, total bytes = 400,000,000)
        queue.push(chunk.clone()).unwrap();
        queue.push(chunk.clone()).unwrap();

        assert_eq!(queue.current_bytes, MAX_QUEUE_BYTES);
        assert_eq!(queue.pending.len(), 2);

        let before = queue.current_bytes;
        // Error should be returned because the queue is full in terms of bytes
        assert_eq!(queue.push("A".repeat(1)), Err(QueueError::QueueFull));
        assert_eq!(queue.current_bytes, before);
    }

    #[test]
    fn test_triage_frees_space() {
        let mut queue = ClipboardQueue::default();
        for i in 0..MAX_QUEUE_ITEMS {
            queue.push(format!("item-{i}")).unwrap();
        }

        let _ = queue.next();
        assert_eq!(queue.pending.len(), MAX_QUEUE_ITEMS - 1);
        assert!(queue.push("replacement".to_string()).is_ok());
    }

    #[test]
    fn test_star_current_moves_item() {
        let mut queue = ClipboardQueue::default();
        let text = "important".to_string();

        queue.push(text.clone()).unwrap();
        let starred_item = queue.star_current().unwrap();

        assert!(queue.pending.is_empty());
        assert_eq!(starred_item.text, text);
        assert_eq!(queue.current_bytes, 0);
    }

    #[test]
    fn test_next_reduces_bytes() {
        let mut queue = ClipboardQueue::default();
        let payload = "A".repeat(1_000_000);

        queue.push(payload).unwrap();
        assert_eq!(queue.current_bytes, 1_000_000);

        let _ = queue.next();
        assert!(queue.pending.is_empty());
        assert_eq!(queue.current_bytes, 0);
    }

    #[test]
    fn test_peek_does_not_remove() {
        let mut queue = ClipboardQueue::default();
        queue.push("Safe Item".to_string()).unwrap();

        // Peeking should leave the item in the queue
        let peeked = queue.peek_current().cloned();
        assert!(peeked.is_some());
        assert_eq!(queue.pending.len(), 1);

        // Starring should remove it
        let starred = queue.star_current();
        assert!(starred.is_some());
        assert_eq!(queue.pending.len(), 0);
    }
}