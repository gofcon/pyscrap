"""Regression tests for error classification, URL masking, and secret placeholder guards."""

from __future__ import annotations

import json
import unittest
import xml.etree.ElementTree as ET

from app.errors import ConfigError, SourceUnavailable, _mask_url, classify
from app.v3.http_fetcher import resolve_secret_placeholders, substitute_placeholders


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


class TestSecurityAndPlaceholders(unittest.TestCase):
    def test_secret_placeholders_block_infrastructure_env_vars(self):
        for forbidden in (
            "ORACLE_PASSWORD",
            "OCI_SECRET_KEY",
            "S3_R2_SECRET_KEY",
            "NEON_DATABASE_URL",
            "DATABASE_URL",
            "ADMIN_API_KEY",
        ):
            with self.assertRaises(PermissionError, msg=f"{forbidden} must be blocked"):
                resolve_secret_placeholders(f"${{{forbidden}}}")

    def test_substitute_placeholders_single_pass_and_backslash_safe(self):
        params = {"A": r":B\1", "B": "SHOULD_NOT_REPLACE"}
        text = "first=:A&second=:B&third=:A2"
        self.assertEqual(
            substitute_placeholders(text, params),
            r"first=:B\1&second=SHOULD_NOT_REPLACE&third=:A2",
        )


if __name__ == "__main__":
    unittest.main()
