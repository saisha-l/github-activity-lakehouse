"""
Explicit PySpark StructType schema for GH Archive events.

The schema covers the most important event types:
  - PushEvent
  - PullRequestEvent
  - IssuesEvent
  - WatchEvent
  - ReleaseEvent
  - ForkEvent / CreateEvent (partial — these have sparse payloads)

All payload sub-fields are nullable so that events with different ``type``
values don't fail with NullPointerException; Spark uses PERMISSIVE mode when
reading with this schema.

Export
------
    GH_EVENT_SCHEMA  — top-level StructType for one GH Archive event row.
"""

from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ---------------------------------------------------------------------------
# Sub-structs
# ---------------------------------------------------------------------------

_ACTOR_SCHEMA = StructType(
    [
        StructField("id", LongType(), True),
        StructField("login", StringType(), True),
        StructField("display_login", StringType(), True),
        StructField("gravatar_id", StringType(), True),
        StructField("url", StringType(), True),
        StructField("avatar_url", StringType(), True),
    ]
)

_REPO_SCHEMA = StructType(
    [
        StructField("id", LongType(), True),
        StructField("name", StringType(), True),
        StructField("url", StringType(), True),
    ]
)

_ORG_SCHEMA = StructType(
    [
        StructField("id", LongType(), True),
        StructField("login", StringType(), True),
        StructField("gravatar_id", StringType(), True),
        StructField("url", StringType(), True),
        StructField("avatar_url", StringType(), True),
    ]
)

# --- Push payload ---
_PUSH_COMMIT_SCHEMA = StructType(
    [
        StructField("sha", StringType(), True),
        StructField("author", StructType(
            [
                StructField("email", StringType(), True),
                StructField("name", StringType(), True),
            ]
        ), True),
        StructField("message", StringType(), True),
        StructField("distinct", BooleanType(), True),
        StructField("url", StringType(), True),
    ]
)

_PUSH_PAYLOAD_SCHEMA = StructType(
    [
        StructField("push_id", LongType(), True),
        StructField("size", IntegerType(), True),
        StructField("distinct_size", IntegerType(), True),
        StructField("ref", StringType(), True),
        StructField("head", StringType(), True),
        StructField("before", StringType(), True),
        # commits is an array but we keep the count only to avoid schema explosion
        StructField("commits_count", IntegerType(), True),
    ]
)

# --- Pull-request payload ---
_PR_SCHEMA = StructType(
    [
        StructField("id", LongType(), True),
        StructField("number", IntegerType(), True),
        StructField("state", StringType(), True),
        StructField("locked", BooleanType(), True),
        StructField("title", StringType(), True),
        StructField("merged", BooleanType(), True),
        StructField("merged_at", TimestampType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True),
        StructField("closed_at", TimestampType(), True),
        StructField("additions", IntegerType(), True),
        StructField("deletions", IntegerType(), True),
        StructField("changed_files", IntegerType(), True),
        StructField("commits", IntegerType(), True),
        StructField("base", StructType(
            [StructField("ref", StringType(), True),
             StructField("sha", StringType(), True)]
        ), True),
        StructField("head", StructType(
            [StructField("ref", StringType(), True),
             StructField("sha", StringType(), True)]
        ), True),
    ]
)

_PR_PAYLOAD_SCHEMA = StructType(
    [
        StructField("action", StringType(), True),
        StructField("number", IntegerType(), True),
        StructField("pull_request", _PR_SCHEMA, True),
    ]
)

# --- Issues payload ---
_ISSUE_SCHEMA = StructType(
    [
        StructField("id", LongType(), True),
        StructField("number", IntegerType(), True),
        StructField("title", StringType(), True),
        StructField("state", StringType(), True),
        StructField("locked", BooleanType(), True),
        StructField("comments", IntegerType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True),
        StructField("closed_at", TimestampType(), True),
    ]
)

_ISSUES_PAYLOAD_SCHEMA = StructType(
    [
        StructField("action", StringType(), True),
        StructField("issue", _ISSUE_SCHEMA, True),
    ]
)

# --- Watch (star) payload ---
_WATCH_PAYLOAD_SCHEMA = StructType(
    [
        StructField("action", StringType(), True),
    ]
)

# --- Release payload ---
_RELEASE_SCHEMA = StructType(
    [
        StructField("id", LongType(), True),
        StructField("tag_name", StringType(), True),
        StructField("name", StringType(), True),
        StructField("draft", BooleanType(), True),
        StructField("prerelease", BooleanType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("published_at", TimestampType(), True),
    ]
)

_RELEASE_PAYLOAD_SCHEMA = StructType(
    [
        StructField("action", StringType(), True),
        StructField("release", _RELEASE_SCHEMA, True),
    ]
)

# --- Fork payload ---
_FORK_PAYLOAD_SCHEMA = StructType(
    [
        StructField("forkee", StructType(
            [
                StructField("id", LongType(), True),
                StructField("name", StringType(), True),
                StructField("full_name", StringType(), True),
                StructField("private", BooleanType(), True),
                StructField("fork", BooleanType(), True),
            ]
        ), True),
    ]
)

# --- Create payload ---
_CREATE_PAYLOAD_SCHEMA = StructType(
    [
        StructField("ref", StringType(), True),
        StructField("ref_type", StringType(), True),
        StructField("master_branch", StringType(), True),
        StructField("description", StringType(), True),
        StructField("pusher_type", StringType(), True),
    ]
)

# ---------------------------------------------------------------------------
# Unified payload — we flatten event-type-specific fields into a single struct
# using the union of all sub-schemas.  Fields absent for a given event type
# will be null.
# ---------------------------------------------------------------------------

PAYLOAD_SCHEMA = StructType(
    [
        # Generic action field (present in most event types)
        StructField("action", StringType(), True),
        # PushEvent
        StructField("push_id", LongType(), True),
        StructField("size", IntegerType(), True),
        StructField("distinct_size", IntegerType(), True),
        StructField("ref", StringType(), True),
        StructField("head", StringType(), True),
        StructField("before", StringType(), True),
        # PullRequestEvent
        StructField("number", IntegerType(), True),
        StructField("pull_request", _PR_SCHEMA, True),
        # IssuesEvent
        StructField("issue", _ISSUE_SCHEMA, True),
        # ReleaseEvent
        StructField("release", _RELEASE_SCHEMA, True),
        # ForkEvent
        StructField("forkee", StructType(
            [
                StructField("id", LongType(), True),
                StructField("name", StringType(), True),
                StructField("full_name", StringType(), True),
                StructField("private", BooleanType(), True),
                StructField("fork", BooleanType(), True),
            ]
        ), True),
        # CreateEvent
        StructField("ref_type", StringType(), True),
        StructField("master_branch", StringType(), True),
        StructField("description", StringType(), True),
        StructField("pusher_type", StringType(), True),
    ]
)

# ---------------------------------------------------------------------------
# Top-level GH Event schema
# ---------------------------------------------------------------------------

GH_EVENT_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("type", StringType(), True),
        StructField("actor", _ACTOR_SCHEMA, True),
        StructField("repo", _REPO_SCHEMA, True),
        StructField("payload", PAYLOAD_SCHEMA, True),
        StructField("public", BooleanType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("org", _ORG_SCHEMA, True),
    ]
)
