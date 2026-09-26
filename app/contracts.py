"""Event payload contracts: immutable JSON Schema documents with version chains.

A contract is a self-contained JSON Schema (Draft 2020-12) registered for an
``event_type`` at a specific ``schema_version``. Versions form a chain via the
optional ``supersedes`` pointer to an existing version of the *same* event
type. Contracts are immutable: a PUT to an existing (event_type, version)
pair must replay the identical document or it is rejected.

Schemas must not reference any external resource: only same-document refs
(starting with ``#``) are allowed, so validation never needs network access.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from jsonschema import Draft202012Validator
from jsonschema.exceptions import SchemaError as JsonschemaSchemaError

# Keywords that can point at another (possibly remote) document. Same-
# document refs ("#...") are in-memory only and therefore permitted.
REMOTE_REF_KEYWORDS = ("$ref", "$dynamicRef", "$recursiveRef")


class ContractError(Exception):
    """Validation failure with JSON-Pointer-annotated error entries."""

    def __init__(self, message: str, errors: list[dict[str, str]]):
        super().__init__(message)
        self.message = message
        self.errors = errors


def escape_pointer(token: Any) -> str:
    """Escape one JSON Pointer reference token (RFC 6901)."""
    return str(token).replace("~", "~0").replace("/", "~1")


def pointer(path: Any) -> str:
    """Render an absolute JSON Pointer from a sequence of object/array steps."""
    if not path:
        return ""
    return "/" + "/".join(escape_pointer(part) for part in path)


def walk(node: Any):
    """Yield (json_pointer, node) for node itself and every nested value."""
    yield "", node
    if isinstance(node, dict):
        for key, value in node.items():
            for sub_pointer, sub_node in walk(value):
                yield "/" + escape_pointer(key) + sub_pointer, sub_node
    elif isinstance(node, list):
        for index, value in enumerate(node):
            for sub_pointer, sub_node in walk(value):
                yield "/" + escape_pointer(index) + sub_pointer, sub_node


def find_external_refs(document: Any) -> list[str]:
    """Return JSON Pointers of every ref-like keyword that is not local."""
    found: list[str] = []
    if not isinstance(document, dict):
        return found
    for path, node in walk(document):
        if isinstance(node, dict):
            for keyword in REMOTE_REF_KEYWORDS:
                if keyword not in node:
                    continue
                target = node[keyword]
                if isinstance(target, str) and not target.startswith("#"):
                    found.append(f"{path}/{keyword}" if path else f"/{keyword}")
    return found


def check_meta(document: Any) -> None:
    """Reject anything that is not a valid Draft 2020-12 schema object."""
    if not isinstance(document, dict):
        raise ContractError(
            "schema must be a JSON Schema object",
            [{"path": "", "error": "document root must be a JSON object"}],
        )
    external = find_external_refs(document)
    if external:
        raise ContractError(
            "schema must not reference external resources",
            [
                {
                    "path": ref,
                    "error": "only same-document refs starting with '#' are allowed",
                }
                for ref in external
            ],
        )
    try:
        Draft202012Validator.check_schema(document)
    except JsonschemaSchemaError as exc:
        raise ContractError(
            "invalid JSON Schema document",
            [{"path": pointer(exc.absolute_path), "error": exc.message}],
        ) from exc


def check_payload(document: Any, payload: Any) -> None:
    """Validate ``payload`` against ``document``; raise ContractError (422)."""
    validator = Draft202012Validator(document)
    failures: list[dict[str, str]] = []
    for error in sorted(validator.iter_errors(payload), key=lambda e: list(e.absolute_path)):
        path = pointer(error.absolute_path)
        if error.validator == "required":
            # For a missing property the instance path points at the parent
            # object; point instead at the absent field itself. jsonschema
            # reports the field name only in the message, quoted.
            missing = error.message.split("'", 2)
            if len(missing) >= 2:
                path = f"{path}/{escape_pointer(missing[1])}"
        failures.append({"path": path, "error": error.message})
    if failures:
        raise ContractError(
            "payload does not satisfy the registered schema", failures
        )


class _Missing:
    """Sentinel: looked-up version is not registered."""

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return "<MISSING>"


MISSING = _Missing()


def validate_chain_link(
    version: str,
    supersedes: str | None,
    get_supersedes: Callable[[str], "str | None | _Missing"],
) -> None:
    """Validate a new chain link for one event_type.

    ``get_supersedes(v)`` returns the stored predecessor of version ``v``,
    None when ``v`` is a root version, or :data:`MISSING` when ``v`` is not
    registered. Missing predecessors, self-references and chains that would
    close a cycle are rejected.
    """
    if supersedes is None:
        return
    if supersedes == version:
        raise ContractError(
            "supersedes cycle detected",
            [
                {
                    "path": "/supersedes",
                    "error": (
                        f"supersedes cycle: version {version!r} cannot "
                        "supersede itself"
                    ),
                }
            ],
        )

    seen: set[str] = set()
    current: str | None = supersedes
    while current is not None:
        if current == version or current in seen:
            raise ContractError(
                "supersedes cycle detected",
                [
                    {
                        "path": "/supersedes",
                        "error": (
                            "supersedes cycle: chain from "
                            f"{supersedes!r} reaches {current!r}"
                        ),
                    }
                ],
            )
        seen.add(current)
        predecessor = get_supersedes(current)
        if predecessor is MISSING:
            raise ContractError(
                "unknown supersedes version",
                [
                    {
                        "path": "/supersedes",
                        "error": (
                            f"version {current!r} is not registered for this "
                            "event_type"
                        ),
                    }
                ],
            )
        current = predecessor
