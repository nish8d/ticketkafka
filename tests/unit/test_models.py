import pytest
from pydantic import ValidationError

from pipeline.models import CHANNELS, TIERS, Ticket


def test_valid_ticket_parses(ticket_dict):
    ticket = Ticket.model_validate(ticket_dict)
    assert ticket.customer_id == "C-0042"
    assert ticket.channel in CHANNELS


def test_json_round_trip(ticket_dict):
    ticket = Ticket.model_validate(ticket_dict)
    assert Ticket.model_validate_json(ticket.model_dump_json()) == ticket


@pytest.mark.parametrize("field", ["ticket_id", "customer_id", "created_at", "channel", "product", "subject", "body"])
def test_missing_field_is_invalid(ticket_dict, field):
    del ticket_dict[field]
    with pytest.raises(ValidationError):
        Ticket.model_validate(ticket_dict)


@pytest.mark.parametrize("field", ["subject", "body", "product"])
def test_whitespace_only_text_is_invalid(ticket_dict, field):
    ticket_dict[field] = "   \n "
    with pytest.raises(ValidationError):
        Ticket.model_validate(ticket_dict)


def test_naive_timestamp_is_invalid(ticket_dict):
    ticket_dict["created_at"] = "2026-09-28T12:00:00"
    with pytest.raises(ValidationError):
        Ticket.model_validate(ticket_dict)


@pytest.mark.parametrize("customer_id", ["42", "C-42", "c-0042", "C-00420"])
def test_bad_customer_id_is_invalid(ticket_dict, customer_id):
    ticket_dict["customer_id"] = customer_id
    with pytest.raises(ValidationError):
        Ticket.model_validate(ticket_dict)


def test_unknown_channel_is_invalid(ticket_dict):
    ticket_dict["channel"] = "fax"
    with pytest.raises(ValidationError):
        Ticket.model_validate(ticket_dict)


def test_unknown_extra_fields_are_ignored(ticket_dict):
    ticket_dict["loyalty_tier"] = "gold"
    ticket = Ticket.model_validate(ticket_dict)
    assert "loyalty_tier" not in ticket.model_dump()


def test_tier_defaults_to_free_for_tickets_written_before_it_existed(ticket_dict):
    assert Ticket.model_validate(ticket_dict).tier == "free"


@pytest.mark.parametrize("tier", TIERS)
def test_known_tiers_are_valid(ticket_dict, tier):
    assert Ticket.model_validate({**ticket_dict, "tier": tier}).tier == tier


def test_unknown_tier_is_invalid(ticket_dict):
    with pytest.raises(ValidationError):
        Ticket.model_validate({**ticket_dict, "tier": "platinum"})
