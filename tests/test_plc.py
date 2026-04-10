"""Tests for PLC directory command."""

from __future__ import annotations

import json
import os
import tempfile
from unittest.mock import patch, MagicMock

from skycoll.commands.plc import _fetch_plc_log, _audit_summary, run


class TestPlcLog:
    """Test PLC log fetching and audit summary."""

    @patch("skycoll.commands.plc.httpx.get")
    def test_fetch_plc_log(self, mock_get):
        """Should fetch and return the operation log."""
        ops = [
            {"op": "create", "did": "did:plc:abc", "createdAt": "2024-01-01T00:00:00Z"},
            {"op": "update", "did": "did:plc:abc", "handle": "alice.bsky.social", "createdAt": "2024-02-01T00:00:00Z"},
        ]
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = ops
        mock_get.return_value = mock_resp

        result = _fetch_plc_log("did:plc:abc")
        assert len(result) == 2

    @patch("skycoll.commands.plc.httpx.get")
    def test_fetch_plc_log_failure(self, mock_get):
        """Should raise RuntimeError on failure."""
        mock_resp = MagicMock()
        mock_resp.status_code = 404
        mock_get.return_value = mock_resp

        import pytest
        with pytest.raises(RuntimeError):
            _fetch_plc_log("did:plc:nonexistent")


class TestAuditSummary:
    """Test audit summary generation."""

    def test_empty_ops(self):
        """Should report no operations."""
        assert "No operations" in _audit_summary([])

    def test_summary_with_ops(self):
        """Should include operation count and extracted info."""
        ops = [
            {
                "op": "create",
                "did": "did:plc:abc",
                "createdAt": "2024-01-01T00:00:00Z",
                "handle": "alice.bsky.social",
                "service": {"serviceEndpoint": "https://bsky.social"},
            },
        ]
        summary = _audit_summary(ops)
        assert "1" in summary
        assert "alice.bsky.social" in summary
        assert "https://bsky.social" in summary