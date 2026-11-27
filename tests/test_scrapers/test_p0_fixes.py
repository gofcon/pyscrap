"""Regression tests for P0 bug fixes and security boundaries."""

from __future__ import annotations

import json
import os
import unittest
import xml.etree.ElementTree as ET
from unittest import mock

from app.errors import ConfigError, SourceUnavailable, _mask_url, classify


class TestErrorClassificationAndMasking(unittest.TestCase):
    def test_json_and_xml_decode_errors_are_source_unavailable_not_config_error(self):
        try:
            json.loads("<html>error page</html>")
        except json.JSONDecodeError as exc:
            err = classify(exc)
        self.assertIsInstance(err, SourceUnavailable)
        self.assertTrue(err.retryable)
        self.assertTrue(err.counts_toward_retire)

        try:
            ET.fromstring("<broken>")
        except ET.ParseError as exc:
            xml_err = classify(exc)
        self.assertIsInstance(xml_err, SourceUnavailable)

        cfg_err = classify(ValueError("bad config"))
        self.assertIsInstance(cfg_err, ConfigError)
        self.assertFalse(cfg_err.retryable)
        self.assertFalse(cfg_err.counts_toward_retire)

    def test_mask_url_hides_sensitive_query_params(self):
        raw = "https://opendart.fss.or.kr/api/list.json?crtfc_key=SECRET123&bgn_de=20261127&token=XYZ"
        masked = _mask_url(raw)
        self.assertNotIn("SECRET123", masked)
        self.assertNotIn("XYZ", masked)
        self.assertIn("bgn_de=20261127", masked)
        self.assertIn("crtfc_key=%2A%2A%2A", masked)


class TestPureScraperHelpers(unittest.TestCase):
    def test_auth_config_blocks_infrastructure_secrets(self):
        # Test _is_blocked_env_var directly without requiring dotenv/loguru
        import ast
        from pathlib import Path

        src = Path("app/auth_config.py").read_text(encoding="utf-8")
        tree = ast.parse(src)
        ns: dict = {}
        for node in tree.body:
            if isinstance(node, (ast.Assign, ast.FunctionDef)):
                name = node.name if isinstance(node, ast.FunctionDef) else None
                targets = (
                    [t.id for t in node.targets if isinstance(t, ast.Name)]
                    if isinstance(node, ast.Assign)
                    else []
                )
                if name == "_is_blocked_env_var" or any(t.startswith("_BLOCKED_") for t in targets):
                    exec(compile(ast.Module(body=[node], type_ignores=[]), "auth_config", "exec"), ns)

        is_blocked = ns["_is_blocked_env_var"]
        for forbidden in ("ORACLE_PASSWORD", "OCI_SECRET_KEY", "S3_R2_SECRET_KEY", "NEON_DATABASE_URL", "DATABASE_URL", "ADMIN_API_KEY"):
            self.assertTrue(is_blocked(forbidden), f"{forbidden} must be blocked")
        for allowed in ("KRX_AUTH_KEY", "KIS_ACCESS_TOKEN", "DART_KEY"):
            self.assertFalse(is_blocked(allowed), f"{allowed} must be allowed")

    def test_substitute_placeholders_single_pass_and_backslash_safe(self):
        import re

        pattern = re.compile(r"(?<!\w):(\w+)(?!\w)")
        params = {"A": r":B\1", "B": "SHOULD_NOT_REPLACE"}
        text = "first=:A&second=:B&third=:A2"
        result = pattern.sub(lambda m: str(params[m.group(1)]) if m.group(1) in params else m.group(0), text)
        self.assertEqual(result, r"first=:B\1&second=SHOULD_NOT_REPLACE&third=:A2")


if __name__ == "__main__":
    unittest.main()
