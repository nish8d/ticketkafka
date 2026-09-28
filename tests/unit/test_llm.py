from types import SimpleNamespace

import httpx
import ollama
import pytest

from pipeline.llm import LLMError, TicketText, build_prompt, generate_ticket_text


class StubClient:
    """Stands in for ollama.Client: returns canned content or raises."""

    def __init__(self, content: str | None = None, exc: Exception | None = None):
        self.content = content
        self.exc = exc
        self.calls: list[dict] = []

    def chat(self, **kwargs):
        self.calls.append(kwargs)
        if self.exc is not None:
            raise self.exc
        return SimpleNamespace(message=SimpleNamespace(content=self.content))


def _call(client):
    return generate_ticket_text(client, "test-model", "angry", "StreamBox TV", "chat")


def test_good_json_becomes_ticket_text():
    client = StubClient('{"subject": "No picture", "body": "The screen stays black."}')
    assert _call(client) == TicketText(subject="No picture", body="The screen stays black.")


def test_requests_structured_json_without_thinking():
    client = StubClient('{"subject": "s", "body": "b"}')
    _call(client)
    kwargs = client.calls[0]
    assert kwargs["model"] == "test-model"
    assert kwargs["format"] == TicketText.model_json_schema()
    assert kwargs["think"] is False


def test_prompt_mentions_persona_product_and_channel():
    prompt = build_prompt("angry", "StreamBox TV", "chat")
    assert "angry" in prompt and "StreamBox TV" in prompt and "chat" in prompt


@pytest.mark.parametrize("content", [
    "not json at all",
    '{"subject": "only a subject"}',
    '{"subject": "   ", "body": "blank subject"}',
    '{"subject": "s", "body": ""}',
])
def test_unusable_output_raises_llm_error(content):
    with pytest.raises(LLMError, match="unusable output"):
        _call(StubClient(content))


@pytest.mark.parametrize("exc", [
    ConnectionError("Failed to connect to Ollama"),
    httpx.ConnectTimeout("timed out"),
    ollama.ResponseError("model 'test-model' not found"),
])
def test_transport_failures_raise_llm_error(exc):
    with pytest.raises(LLMError, match="ollama call failed"):
        _call(StubClient(exc=exc))
