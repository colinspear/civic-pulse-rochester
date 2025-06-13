from __future__ import annotations

from typing import Optional, Dict, Any

def extract_tract_from_event(event: Dict[str, Any]) -> Optional[str]:
    """Return the tract id from a deck.gl selection event."""
    if (
        isinstance(event, dict)
        and event.get("selection")
        and event["selection"]["objects"].get("tract-layer")
    ):
        first = event["selection"]["objects"]["tract-layer"][0]
        tract_id = first.get("properties", {}).get("tract") or first.get("properties", {}).get("GEOID")
        if tract_id:
            return str(tract_id)
    return None
