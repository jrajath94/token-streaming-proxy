"""Tests for CLI argument parsing and configuration."""

from __future__ import annotations

from unittest.mock import patch

from token_streaming_proxy.cli import main


class TestCLI:
    """Tests for CLI argument parsing."""

    def test_parses_required_upstream(self) -> None:
        """--upstream argument is parsed into config."""
        with patch("token_streaming_proxy.cli.uvicorn") as mock_uvicorn:
            mock_uvicorn.run = lambda *a, **kw: None
            main(["--upstream", "http://localhost:11434"])

    def test_parses_all_arguments(self) -> None:
        """All CLI arguments are parsed correctly."""
        with patch("token_streaming_proxy.cli.uvicorn") as mock_uvicorn:
            calls = {}

            def capture_run(app, **kwargs):
                calls.update(kwargs)

            mock_uvicorn.run = capture_run
            main([
                "--upstream", "http://api.example.com",
                "--host", "127.0.0.1",
                "--port", "9090",
                "--timeout", "60.0",
                "--max-connections", "50",
                "--max-buffer", "2048",
                "--heartbeat", "5.0",
            ])

            assert calls["host"] == "127.0.0.1"
            assert calls["port"] == 9090

    def test_verbose_flag(self) -> None:
        """--verbose sets debug log level."""
        with patch("token_streaming_proxy.cli.uvicorn") as mock_uvicorn:
            calls = {}

            def capture_run(app, **kwargs):
                calls.update(kwargs)

            mock_uvicorn.run = capture_run
            main(["--upstream", "http://localhost:8000", "-v"])

            assert calls["log_level"] == "debug"

    def test_default_port(self) -> None:
        """Default port is 8080."""
        with patch("token_streaming_proxy.cli.uvicorn") as mock_uvicorn:
            calls = {}

            def capture_run(app, **kwargs):
                calls.update(kwargs)

            mock_uvicorn.run = capture_run
            main(["--upstream", "http://localhost:8000"])

            assert calls["port"] == 8080
