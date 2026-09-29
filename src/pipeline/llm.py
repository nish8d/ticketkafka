"""Thin wrapper around Ollama: writes ticket text (generator) and classifies tickets (enricher)."""
import httpx
import ollama
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from pipeline.models import Category, Priority, Ticket

PERSONAS: tuple[str, ...] = (
    "angry",
    "confused",
    "polite",
    "impatient",
    "sarcastic",
    "elderly and unfamiliar with technology",
)


class TicketText(BaseModel):
    """The only part of a ticket the LLM writes."""

    model_config = ConfigDict(str_strip_whitespace=True)

    subject: str = Field(min_length=1, max_length=200)
    body: str = Field(min_length=1)


class LLMError(RuntimeError):
    """The LLM was unreachable or returned something we can't use."""


def build_prompt(persona: str, product: str, channel: str) -> str:
    return (
        f"Write one realistic customer support ticket from a {persona} customer "
        f"about the product '{product}', sent via {channel}. "
        "Pick one concrete problem: a billing issue, a bug, account access, shipping, or a feature request. "
        'Reply only with JSON: {"subject": "<short subject line>", '
        '"body": "<2-5 sentences written by the customer>"}.'
    )


def make_client(host: str, timeout: float = 120.0) -> ollama.Client:
    return ollama.Client(host=host, timeout=timeout)


def generate_ticket_text(client, model: str, persona: str, product: str, channel: str) -> TicketText:
    try:
        response = client.chat(
            model=model,
            messages=[{"role": "user", "content": build_prompt(persona, product, channel)}],
            # Passing a JSON schema makes Ollama constrain output to that shape.
            format=TicketText.model_json_schema(),
            # Skip "thinking" on reasoning models: much faster, and we don't need it here.
            think=False,
            options={"temperature": 0.9},
        )
    except (ollama.ResponseError, httpx.HTTPError, ConnectionError) as exc:
        raise LLMError(f"ollama call failed: {exc}") from exc
    try:
        return TicketText.model_validate_json(response.message.content)
    except ValidationError as exc:
        raise LLMError(f"model returned unusable output: {exc}") from exc


class Classification(BaseModel):
    """The LLM's half of an EnrichedTicket."""

    model_config = ConfigDict(str_strip_whitespace=True)

    category: Category
    priority: Priority
    sentiment: float = Field(ge=-1.0, le=1.0)
    summary: str = Field(min_length=1, max_length=300)


def build_classify_prompt(ticket: Ticket) -> str:
    return (
        "You triage customer support tickets. Classify this one.\n"
        f"Product: {ticket.product}\nChannel: {ticket.channel}\n"
        f"Subject: {ticket.subject}\nBody: {ticket.body}\n\n"
        "Reply only with JSON: "
        '{"category": "billing" | "technical" | "account" | "other", '
        '"priority": "low" | "medium" | "high" | "urgent", '
        '"sentiment": <number from -1.0 (furious) to 1.0 (delighted)>, '
        '"summary": "<one sentence>"}. '
        "Use urgent only when the customer is blocked right now or is losing money."
    )


def classify_ticket(client, model: str, ticket: Ticket) -> Classification:
    try:
        response = client.chat(
            model=model,
            messages=[{"role": "user", "content": build_classify_prompt(ticket)}],
            format=Classification.model_json_schema(),
            think=False,
            # Temperature 0: a classifier should give the same answer for the same ticket.
            options={"temperature": 0},
        )
    except (ollama.ResponseError, httpx.HTTPError, ConnectionError) as exc:
        raise LLMError(f"ollama call failed: {exc}") from exc
    try:
        return Classification.model_validate_json(response.message.content)
    except ValidationError as exc:
        raise LLMError(f"model returned unusable output: {exc}") from exc
