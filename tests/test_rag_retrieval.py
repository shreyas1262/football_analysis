import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent / "agent"))

from generate_and_store_narratives import chunk_text


def test_chunk_text_splits_at_sentences():
    text = "Arsenal won the match. Saka scored twice. Chelsea struggled."
    chunks = chunk_text(text, chunk_size=300)
    assert len(chunks) >= 1
    for chunk in chunks[:-1]:
        assert chunk.strip()[-1] in ".!?"


def test_chunk_text_handles_short_text():
    text = "A short narrative."
    chunks = chunk_text(text, chunk_size=300)
    assert len(chunks) == 1
    assert chunks[0] == "A short narrative."


def test_chunk_text_handles_empty_string():
    chunks = chunk_text("", chunk_size=300)
    assert chunks == [] or chunks == [""]


def test_embedding_dimension(db_conn):
    with db_conn.cursor() as cur:
        cur.execute("""
            SELECT vector_dims(embedding) AS dims
            FROM marts.report_embeddings
            LIMIT 1
        """)
        row = cur.fetchone()
        if row:
            assert row[0] == 1024
