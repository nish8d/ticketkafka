"""Message shapes shared by every stage."""
from typing import Literal, get_args
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

Channel = Literal["email", "chat", "phone"]
CHANNELS: tuple[str, ...] = get_args(Channel)

# Added in schema v2. Derived from the customer, so one customer always has the same tier.
Tier = Literal["free", "pro", "enterprise"]
TIERS: tuple[str, ...] = get_args(Tier)

# Written by the enricher (stage 5).
Category = Literal["billing", "technical", "account", "other"]
CATEGORIES: tuple[str, ...] = get_args(Category)
Priority = Literal["low", "medium", "high", "urgent"]
PRIORITIES: tuple[str, ...] = get_args(Priority)

PRODUCTS: tuple[str, ...] = (
    "SmartHome Hub",
    "CloudDrive Pro",
    "FitTrack Watch",
    "StreamBox TV",
    "PayWallet App",
)


class Ticket(BaseModel):
    # extra="ignore": a consumer that tolerates unknown fields lets producers add
    # fields later without breaking it (forward compatibility).
    model_config = ConfigDict(extra="ignore", frozen=True, str_strip_whitespace=True)

    ticket_id: UUID
    customer_id: str = Field(pattern=r"^C-\d{4}$")
    created_at: AwareDatetime
    channel: Channel
    product: str = Field(min_length=1)
    subject: str = Field(min_length=1, max_length=200)
    body: str = Field(min_length=1)
    # The default mirrors the Avro default in ticket.v2.avsc: tickets written before v2 are "free".
    tier: Tier = "free"


class EnrichedTicket(Ticket):
    """A ticket plus what the LLM made of it. Produced to the routed topics by the enricher."""

    category: Category
    priority: Priority
    sentiment: float = Field(ge=-1.0, le=1.0)
    summary: str = Field(min_length=1, max_length=300)
    enriched_at: AwareDatetime
    model: str = Field(min_length=1)
