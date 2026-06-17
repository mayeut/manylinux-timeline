import os
from pathlib import Path
from typing import Final

import nox

HERE: Final[Path] = Path(__file__).resolve(strict=True).parent
PYTHON_VERSION: Final[str] = HERE.joinpath(".python-version").read_text().strip()

nox.needs_version = ">=2025.11.12"
nox.options.default_venv_backend = "uv|virtualenv"
nox.options.pythons = [PYTHON_VERSION]
nox.options.sessions = ["run"]
nox.options.reuse_existing_virtualenvs = True
nox.options.error_on_missing_interpreters = True


@nox.session(python=PYTHON_VERSION)
def lint(session: nox.Session) -> None:
    """Run the linter."""
    session.install("-U", "pre-commit")
    session.run("pre-commit", "run", "--all-files", *session.posargs)


@nox.session(python=PYTHON_VERSION)
def update_requirements(session: nox.Session) -> None:
    """Update requirements.txt."""

    if session.venv_backend != "uv":
        session.install("uv>=0.9")

    env = os.environ.copy()
    # UV_CUSTOM_COMPILE_COMMAND is an uv option that tells users how to
    # regenerate the constraints files
    env["UV_CUSTOM_COMPILE_COMMAND"] = f"nox -s {session.name}"
    session.run(
        "uv",
        "pip",
        "compile",
        "--upgrade",
        "--generate-hashes",
        "requirements.in",
        env=env,
    )


@nox.session(python=PYTHON_VERSION)
def run(session: nox.Session) -> None:
    """Run manylinux-timeline."""
    session.install(
        "--only-binary",
        ":all:",
        "--require-hashes",
        "-r",
        "requirements.txt",
    )
    session.run("python", "update.py", *session.posargs)


@nox.session(python=PYTHON_VERSION)
def serve(session: nox.Session) -> None:
    session.run("python", "-m", "http.server", "-d", "build")


@nox.session(python=PYTHON_VERSION, venv_backend="none")
def timestamp(session: nox.Session) -> None:  # noqa: ARG001
    """Get timestamp for PyPI package cache on GHA"""
    from datetime import UTC, datetime  # noqa: PLC0415

    print(datetime.now(UTC).isoformat())  # noqa: T201
