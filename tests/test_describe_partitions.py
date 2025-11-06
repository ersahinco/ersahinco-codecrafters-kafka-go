#!/usr/bin/env python3
# tests/test_describe_partitions.py
import socket
import struct
import uuid


# ---------- Kafka flexible (v2/v0) helpers ----------

def uvarint_encode(x: int) -> bytes:
    out = bytearray()
    while True:
        if (x & ~0x7F) == 0:
            out.append(x)
            return bytes(out)
        out.append((x & 0x7F) | 0x80)
        x >>= 7


def uvarint_decode(buf: bytes, i: int):
    x, s = 0, 0
    while True:
        b = buf[i]
        i += 1
        if b < 0x80:
            x |= b << s
            return x, i
        x |= (b & 0x7F) << s
        s += 7


def compact_str(s: str) -> bytes:
    b = s.encode()
    return uvarint_encode(len(b) + 1) + b


def compact_nullable_str(s: str | None) -> bytes:
    if s is None:
        return uvarint_encode(0)  # null
    return compact_str(s)


def compact_arr_len(n: int) -> bytes:
    return uvarint_encode(n + 1)


def compact_i32_array(xs: list[int]) -> bytes:
    out = bytearray()
    out += compact_arr_len(len(xs))
    for v in xs:
        out += struct.pack(">i", v)
    return bytes(out)


# ---------- Request builder: DescribeTopicPartitions v0 with header v2 ----------

def build_dtp_v0_request(topics: list[str], limit: int, corr_id: int, client_id: str = "kfk-cli") -> bytes:
    api_key, api_ver = 75, 0  # DescribeTopicPartitions v0

    # Header v2:
    # int16 api_key, int16 api_version, int32 correlation_id,
    # COMPACT_NULLABLE_STRING client_id, TAG_BUFFER (uvarint)
    hdr = bytearray()
    hdr += struct.pack(">hhI", api_key, api_ver, corr_id)
    hdr += compact_nullable_str(client_id)  # client id
    hdr += uvarint_encode(0)                # header TAG_BUFFER (empty)

    # Body (flexible v0):
    # topics: COMPACT_ARRAY<TopicRequest(name, TAGS)>
    body = bytearray()
    body += compact_arr_len(len(topics))
    for t in topics:
        body += compact_str(t)
        body += uvarint_encode(0)  # TopicRequest TAGS (empty)
    # response_partition_limit
    body += struct.pack(">i", limit)
    # cursor: null (OPTION<Cursor> → 0xFF)
    body += b"\xff"
    # body TAG_BUFFER
    body += uvarint_encode(0)

    msg = struct.pack(">I", len(hdr) + len(body)) + bytes(hdr) + bytes(body)
    return msg


# ---------- Response parsing (only fields we implemented) ----------

def recv_exact(sock: socket.socket, n: int) -> bytes:
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("socket closed early")
        buf.extend(chunk)
    return bytes(buf)


def parse_response(buf: bytes):
    """Parse response frame, header v1, and DescribeTopicPartitions v0 body."""
    # Frame header
    if len(buf) < 4:
        raise ValueError("short frame")
    size = struct.unpack_from(">I", buf, 0)[0]
    needed = 4 + size
    if len(buf) < needed:
        raise ValueError(f"incomplete frame: have {len(buf)}, need {needed}")

    # Response header v1: corr_id (int32), header TAGS (uvarint)
    corr_id = struct.unpack_from(">I", buf, 4)[0]
    _, i = uvarint_decode(buf, 8)  # header tags (we expect 0)

    # Body (DescribeTopicPartitions v0):
    # throttle_time_ms
    throttle = struct.unpack_from(">i", buf, i)[0]; i += 4

    # topics: COMPACT_ARRAY<Topic>
    ln1, i = uvarint_decode(buf, i)
    n_topics = max(ln1 - 1, 0)

    topics = []
    for _ in range(n_topics):
        # Topic:
        # error_code, name, topic_id(16), is_internal, partitions, ops, TAGS
        err = struct.unpack_from(">h", buf, i)[0]; i += 2
        name, i = parse_compact_str(buf, i)
        topic_id_bytes = buf[i:i+16]; i += 16
        topic_id = str(uuid.UUID(bytes=bytes(topic_id_bytes)))
        is_internal = buf[i]; i += 1

        # partitions: COMPACT_ARRAY<Partition>
        ln1p, i = uvarint_decode(buf, i)
        n_parts = max(ln1p - 1, 0)

        parts = []
        for _ in range(n_parts):
            perr = struct.unpack_from(">h", buf, i)[0]; i += 2
            pidx = struct.unpack_from(">i", buf, i)[0]; i += 4
            leader = struct.unpack_from(">i", buf, i)[0]; i += 4
            epoch = struct.unpack_from(">i", buf, i)[0]; i += 4
            # arrays: replicas, isr
            replicas, i = parse_compact_i32_array(buf, i)
            isr, i = parse_compact_i32_array(buf, i)
            # eligible_leader_replicas (empty), last_known_elr (empty), offline_replicas (empty)
            _, i = uvarint_decode(buf, i)
            _, i = uvarint_decode(buf, i)
            _, i = uvarint_decode(buf, i)
            # partition TAGS
            _, i = uvarint_decode(buf, i)
            parts.append({"index": pidx, "error": perr, "leader": leader, "epoch": epoch,
                          "replicas": replicas, "isr": isr})

        auth_ops = struct.unpack_from(">i", buf, i)[0]; i += 4
        # topic TAGS
        _, i = uvarint_decode(buf, i)

        topics.append({
            "name": name,
            "error": err,
            "topic_id": topic_id,
            "is_internal": bool(is_internal),
            "partitions": parts,
            "auth_ops": auth_ops
        })

    # next_cursor: null => 0xFF
    next_cursor_is_null = buf[i] == 0xff; i += 1
    # response TAGS
    _, i = uvarint_decode(buf, i)

    return {
        "size": size,
        "corr_id": corr_id,
        "throttle_ms": throttle,
        "topics": topics,
        "next_cursor_null": next_cursor_is_null
    }


def parse_compact_str(buf: bytes, i: int):
    ln1, i = uvarint_decode(buf, i)
    n = ln1 - 1
    s = buf[i:i+n].decode() if n > 0 else ""
    i += max(n, 0)
    return s, i


def parse_compact_i32_array(buf: bytes, i: int):
    ln1, i = uvarint_decode(buf, i)
    n = ln1 - 1
    out = []
    for _ in range(max(n, 0)):
        v = struct.unpack_from(">i", buf, i)[0]; i += 4
        out.append(v)
    return out, i


# ---------- Runner ----------

def send_and_parse(topics, limit, corr):
    req = build_dtp_v0_request(topics, limit, corr)
    s = socket.create_connection(("127.0.0.1", 9092), timeout=2.0)
    s.sendall(req)

    # read response frame header first to know size
    hdr = recv_exact(s, 4)
    size = struct.unpack(">I", hdr)[0]
    body = recv_exact(s, size)
    s.close()

    parsed = parse_response(hdr + body)
    print(f"corr_id=0x{parsed['corr_id']:08x}, throttle={parsed['throttle_ms']}ms, next_cursor_null={parsed['next_cursor_null']}")
    for t in parsed["topics"]:
        pidxs = [p["index"] for p in t["partitions"]]
        perrs = [p["error"] for p in t["partitions"]]
        print(f"- topic={t['name']!r} err={t['error']} id={t['topic_id']} parts={pidxs} errs={perrs}")
    return parsed


if __name__ == "__main__":
    print("== Unknown topic ==")
    send_and_parse(["unknown-topic"], limit=1, corr=0x33445566)

    print("\n== Single known topic: alpha ==")
    send_and_parse(["alpha"], limit=10, corr=0xA1B2C3D4)

    print("\n== Multiple topics (unordered request, sorted response) ==")
    send_and_parse(["beta", "alpha"], limit=10, corr=0x01020304)
