//! SSE event framing for the two translating stream relays.

/// Longest delimiter (`\r\n\r\n`) minus one: how far a search that found
/// nothing backs up, so a delimiter split across pushes is still found.
const MAX_DELIM_TAIL: usize = 3;

/// Splits an SSE byte stream into events at blank lines.
///
/// The SSE spec lets a line end in `\n`, `\r\n` or `\r`, so the blank line
/// between events is `\n\n`, `\r\n\r\n` or `\r\r`; accepting only `\n\n`
/// never splits a CRLF-framed stream. Each search resumes where the last one
/// stopped, so a large event arriving in many chunks is scanned about once,
/// where a rescan from byte 0 per chunk is quadratic in its size. Events come
/// out with every line ending as `\n`: consumers split field lines with
/// `str::lines()`, which does not break on a lone `\r`.
#[derive(Default)]
pub(crate) struct SseEventSplitter {
    buf: Vec<u8>,
    /// Start of the first unconsumed event in `buf`.
    start: usize,
    /// No delimiter starts in `start..resume`.
    resume: usize,
    /// Bytes read by the boundary search, for the linear-scan test.
    #[cfg(test)]
    examined: usize,
}

impl SseEventSplitter {
    pub(crate) fn push(&mut self, chunk: &[u8]) {
        // Drop consumed events once per push rather than once per event.
        if self.start > 0 {
            self.buf.drain(..self.start);
            self.resume -= self.start;
            self.start = 0;
        }
        self.buf.extend_from_slice(chunk);
    }

    /// The next complete event, without its delimiter.
    pub(crate) fn next_event(&mut self) -> Option<String> {
        for i in self.resume..self.buf.len() {
            if let Some(delim) = self.delimiter_len_at(i) {
                let event = decode(&self.buf[self.start..i]);
                self.start = i + delim;
                self.resume = self.start;
                return Some(event);
            }
        }
        self.resume = self
            .buf
            .len()
            .saturating_sub(MAX_DELIM_TAIL)
            .max(self.start);
        None
    }

    /// The unterminated tail: whatever follows the last complete event.
    pub(crate) fn remainder(&self) -> String {
        decode(&self.buf[self.start..])
    }

    /// Length of the delimiter starting at `i`; `None` if there is none, or
    /// not all of it has arrived yet.
    fn delimiter_len_at(&mut self, i: usize) -> Option<usize> {
        match self.byte(i)? {
            b'\n' => (self.byte(i + 1)? == b'\n').then_some(2),
            b'\r' => match self.byte(i + 1)? {
                b'\r' => Some(2),
                b'\n' => (self.byte(i + 2)? == b'\r' && self.byte(i + 3)? == b'\n').then_some(4),
                _ => None,
            },
            _ => None,
        }
    }

    fn byte(&mut self, i: usize) -> Option<u8> {
        #[cfg(test)]
        {
            self.examined += 1;
        }
        self.buf.get(i).copied()
    }
}

/// `bytes` as text, `\r\n` and lone `\r` line endings turned into `\n`.
fn decode(bytes: &[u8]) -> String {
    let text = String::from_utf8_lossy(bytes);
    if !text.contains('\r') {
        return text.into_owned();
    }
    text.replace("\r\n", "\n").replace('\r', "\n")
}

#[cfg(test)]
mod tests;
