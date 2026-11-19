"""Loading a JSON object that a map setting (image map, rule queue map) is given as."""

import json
from typing import Any, Dict, List, Tuple


class _Object(dict):
    """A parsed JSON object that remembers the keys it was given more than once."""

    duplicates: List[str]


def _object(pairs: List[Tuple[str, Any]]) -> _Object:
    """A JSON object hook that records a key given twice (JSON keeps the last)."""
    obj = _Object()
    obj.duplicates = []
    for key, value in pairs:
        if key in obj:
            obj.duplicates.append(key)
        obj[key] = value
    return obj


def load_json_object(text: str, noun: str, what: str) -> Dict[str, Any]:
    """Parse ``text`` as a JSON object, refusing a key given twice.

    Only the object's own keys are checked for duplicates, so a duplicate inside a
    nested value is left for the caller's value check to refuse. ``noun`` names what
    the keys are (``"image"``), ``what`` names the map (``"an image map"``); both are
    for error messages. Raises ``ValueError`` (``json.JSONDecodeError`` is one).
    """
    try:
        data = json.loads(text, object_pairs_hook=_object)
    except RecursionError as e:
        raise ValueError(f"{what} is nested too deeply") from e
    if not isinstance(data, dict):
        raise ValueError(f"{what} must be a JSON object")
    if data.duplicates:
        raise ValueError(f"{noun} {data.duplicates[0]!r} is in the map twice")
    return dict(data)
