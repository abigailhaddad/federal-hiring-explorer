"""
Guards the retry-cron wiring in .github/workflows/rebuild.yml.

The rebuild runs on two schedules: a primary and a later retry that no-ops if
the primary already succeeded. The `check` guard identifies the primary by
comparing github.event.schedule against a cron string written out longhand,
because a workflow cannot reference its own on.schedule entries by index.

That makes the two copies of the primary cron a silent drift hazard: change the
schedule and forget the condition, and the guard attaches to the retry instead.
Then the primary run pays for a second runner acquisition it does not need, and
the retry — the run that is supposed to be cheap — skips the guard and rebuilds
unconditionally. Nothing fails loudly; the workflow just stops doing the thing
it was restructured to do.

Parsed with regexes rather than a YAML library so the test needs no dependency
beyond pytest (CI installs only duckdb, pyarrow and pytest).
"""

import re
from pathlib import Path

WORKFLOW = Path(__file__).parent.parent / ".github" / "workflows" / "rebuild.yml"
TEXT = WORKFLOW.read_text()

CRONS = re.findall(r"^\s*-\s*cron:\s*'([^']+)'", TEXT, re.MULTILINE)
GUARD_LITERALS = re.findall(r"github\.event\.schedule\s*!=\s*'([^']+)'", TEXT)


def test_workflow_exists():
    assert WORKFLOW.exists(), f"{WORKFLOW} is missing"


def test_has_a_primary_and_at_least_one_retry():
    assert len(CRONS) >= 2, f"expected a primary plus a retry cron, found {CRONS}"


def test_guard_condition_names_the_primary_cron():
    """The one the guard excludes must be the FIRST schedule entry."""
    assert len(GUARD_LITERALS) == 1, (
        f"expected exactly one github.event.schedule != '...' guard, "
        f"found {GUARD_LITERALS}"
    )
    assert GUARD_LITERALS[0] == CRONS[0], (
        f"guard excludes {GUARD_LITERALS[0]!r} but the primary cron is "
        f"{CRONS[0]!r} — the guard is attached to the wrong run"
    )


def test_crons_avoid_the_top_of_the_hour():
    """:00 is the most contended minute for GitHub's scheduler."""
    for cron in CRONS:
        minute = cron.split()[0]
        assert minute != "0", (
            f"cron {cron!r} fires on the hour, where the scheduled-workflow "
            "backlog is worst — this is what the 2026-08-10 failure was about"
        )


def test_retries_are_serialized_against_the_primary():
    """Without a concurrency group a retry can race the run it is checking on."""
    assert re.search(r"^concurrency:", TEXT, re.MULTILINE), (
        "rebuild.yml needs a concurrency group so the retry cron waits for an "
        "in-flight primary run instead of racing its commit and push"
    )
