import base64
import struct

MAGIC = "CZIP0001"

HEADER_END = "===== END HEADER ====="
CONTENT_START = "===== ORIGINAL CONTENT ====="
CONTENT_END = "===== END ORIGINAL CONTENT ====="
CHUNKS_START = "===== COMPRESSED CHUNKS ====="
CHUNKS_END = "===== END COMPRESSED CHUNKS ====="


def encode_compressed_data(data):
    return base64.b64encode(data).decode("ascii")


def decode_compressed_data(data):
    return base64.b64decode(data.encode("ascii"))


def create_header(
    filename,
    original_size,
    chunk_size,
    chunk_count
):
    return (
        f"{MAGIC}\n"
        f"FILE: {filename}\n"
        f"ORIGINAL SIZE: {original_size}\n"
        f"CHUNK SIZE: {chunk_size}\n"
        f"CHUNKS: {chunk_count}\n"
        f"{HEADER_END}\n"
    )


def create_content_section(original_data):
    try:
        text = original_data.decode("utf-8")
    except UnicodeDecodeError:
        text = original_data.decode("utf-8", errors="replace")

    return (
        f"{CONTENT_START}\n"
        f"{text}\n"
        f"{CONTENT_END}\n"
    )


def create_chunk_record(
    chunk_id,
    original_size,
    compressed_data
):
    encoded = encode_compressed_data(compressed_data)

    return (
        f"CHUNK {chunk_id}\n"
        f"ORIGINAL SIZE: {original_size}\n"
        f"COMPRESSED SIZE: {len(compressed_data)}\n"
        f"DATA: {encoded}\n"
    )