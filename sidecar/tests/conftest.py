"""Autouse fixtures for classify sidecar tests."""

from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def _classify_auth_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    """Default unit tests skip Bearer checks; auth coverage is in test_auth.py."""
    import app

    monkeypatch.setattr(app, "CLASSIFY_AUTH_DISABLED", True)
