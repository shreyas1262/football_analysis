from football_analytics.agent.generate_and_store_narratives import chunk_text


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
    assert chunks[0] == text
