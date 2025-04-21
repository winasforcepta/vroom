pub struct RingBuffer {
    buffer: Vec<u16>,
    capacity: usize,
    head: usize,
    count: usize,
}

impl RingBuffer {
    pub fn new(capacity: usize) -> Self {
        // Pre-allocate the vector with default values.
        RingBuffer {
            buffer: vec![0; capacity],
            capacity,
            head: 0,
            count: 0,
        }
    }

    pub fn insert(&mut self, value: u16) -> bool {
        if self.count == self.capacity {
            return false; // Buffer full.
        }
        // Tail index is where the new element should be inserted.
        let tail_index = (self.head + self.count) % self.capacity;
        self.buffer[tail_index] = value;
        self.count += 1;
        true
    }

    pub fn remove(&mut self, value: u16) -> bool {
        if self.count == 0 {
            return false; // Buffer empty.
        }
        // Search for the value among the valid elements.
        for i in 0..self.count {
            let index = (self.head + i) % self.capacity;
            if self.buffer[index] == value {
                // Identify the tail index (last valid element).
                let tail_index = (self.head + self.count - 1) % self.capacity;
                // Swap the found element with the tail if they differ.
                if index != tail_index {
                    self.buffer.swap(index, tail_index);
                }
                // Remove the tail element.
                self.count -= 1;
                return true;
            }
        }
        false // Value not found.
    }

    /// Returns the number of elements currently stored in the buffer.
    pub fn len(&self) -> usize {
        self.count
    }

    /// Returns true if the buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Returns true if the buffer is full.
    pub fn is_full(&self) -> bool {
        self.count == self.capacity
    }
}
