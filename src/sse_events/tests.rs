use super::*;

const DELIMITERS: [&str; 3] = ["\n\n", "\r\n\r\n", "\r\r"];

fn drain(s: &mut SseEventSplitter) -> Vec<String> {
    std::iter::from_fn(|| s.next_event()).collect()
}

#[test]
fn each_delimiter_ends_an_event_and_is_consumed_whole() {
    for d in DELIMITERS {
        let mut s = SseEventSplitter::default();
        s.push(format!("event: a{d}data: 1{d}data: 2{d}data: 3").as_bytes());
        assert_eq!(
            drain(&mut s),
            ["event: a", "data: 1", "data: 2"],
            "delimiter {d:?}"
        );
        assert_eq!(s.remainder(), b"data: 3", "delimiter {d:?}");
    }
}

#[test]
fn crlf_field_lines_stay_inside_the_event() {
    let mut s = SseEventSplitter::default();
    s.push(b"event: x\r\ndata: {}\r\n\r\ndata: y\r\n\r\n");
    assert_eq!(drain(&mut s), ["event: x\r\ndata: {}", "data: y"]);
    assert!(s.remainder().is_empty());
}

#[test]
fn delimiter_split_across_pushes_yields_one_boundary() {
    for d in DELIMITERS {
        for k in 1..d.len() {
            let mut s = SseEventSplitter::default();
            // A consumed event first, so the second push also compacts.
            s.push(format!("data: z{d}data: a{}", &d[..k]).as_bytes());
            assert_eq!(drain(&mut s), ["data: z"], "{d:?} split at {k}");
            s.push(format!("{}data: b", &d[k..]).as_bytes());
            assert_eq!(drain(&mut s), ["data: a"], "{d:?} split at {k}");
            assert_eq!(s.remainder(), b"data: b", "{d:?} split at {k}");
        }
    }
}

#[test]
fn large_event_in_small_pushes_is_scanned_linearly() {
    const EVENT_LEN: usize = 4 << 20;
    const PUSH: usize = 8 << 10;
    let event = format!("data: {}", "x".repeat(EVENT_LEN - 6));
    let mut s = SseEventSplitter::default();
    for piece in event.as_bytes().chunks(PUSH) {
        s.push(piece);
        assert_eq!(s.next_event(), None);
    }
    s.push(b"\n\n");
    assert_eq!(s.next_event().as_deref(), Some(event.as_str()));
    assert!(
        s.examined <= 2 * EVENT_LEN,
        "examined {} bytes for a {EVENT_LEN}-byte event; a rescan per push reads ~256x",
        s.examined
    );
}
