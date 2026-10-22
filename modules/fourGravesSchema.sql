-- Additive, idempotent startup migration. No writes to Trick or Treat tables.
CREATE TABLE IF NOT EXISTS four_graves_events (
 id BIGSERIAL PRIMARY KEY, event_key TEXT NOT NULL, guild_id TEXT NOT NULL,
 source_event_key TEXT NOT NULL,
 state TEXT NOT NULL DEFAULT 'SETUP' CHECK(state IN ('SETUP','OPEN','CLOSED_COUNTDOWN','BUTTONS_ACTIVE','REVEALED')),
 config JSONB NOT NULL DEFAULT '{}', panel_channel_id TEXT, panel_message_id TEXT,
 opened_at TIMESTAMPTZ, closed_at TIMESTAMPTZ, deadline TIMESTAMPTZ,
 generation INTEGER NOT NULL DEFAULT 0, snapshot JSONB, snapshot_hash TEXT,
 snapshot_id TEXT, last_export_at TIMESTAMPTZ,
 created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
 UNIQUE(event_key,guild_id)
);
CREATE TABLE IF NOT EXISTS four_graves_graves (
 event_id BIGINT NOT NULL REFERENCES four_graves_events(id), grave_id TEXT NOT NULL,
 fixed_game TEXT NOT NULL,
 role_id TEXT, channel_id TEXT, message_id TEXT,
 decision JSONB, reveal_stage INTEGER NOT NULL DEFAULT 0 CHECK(reveal_stage BETWEEN 0 AND 3),
 next_reveal_at TIMESTAMPTZ, revealed_at TIMESTAMPTZ,
 PRIMARY KEY(event_id,grave_id),
 CHECK((grave_id,fixed_game) IN (('barry_buckets','Wheel of Names'),('barbara_bowkins','Squigwreck'),('cletus_von_wrinkles','Reverse Ugly City'),('scuba_steve_3000','Duck Race')))
);
CREATE TABLE IF NOT EXISTS four_graves_allocations (
 id BIGSERIAL PRIMARY KEY, event_id BIGINT NOT NULL, grave_id TEXT NOT NULL,
 discord_id TEXT NOT NULL, username TEXT NOT NULL, display_name TEXT NOT NULL,
 quantity INTEGER NOT NULL CHECK(quantity>0), idempotency_key TEXT NOT NULL UNIQUE,
 source_counts JSONB NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
 FOREIGN KEY(event_id,grave_id) REFERENCES four_graves_graves(event_id,grave_id)
);
CREATE INDEX IF NOT EXISTS four_graves_allocations_user ON four_graves_allocations(event_id,discord_id);
CREATE TABLE IF NOT EXISTS four_graves_operations (
 event_id BIGINT NOT NULL REFERENCES four_graves_events(id), operation_key TEXT NOT NULL,
 attempts INTEGER NOT NULL DEFAULT 0, last_error TEXT, completed_at TIMESTAMPTZ,
 PRIMARY KEY(event_id,operation_key)
);
CREATE TABLE IF NOT EXISTS four_graves_audit (
 id BIGSERIAL PRIMARY KEY, event_id BIGINT NOT NULL REFERENCES four_graves_events(id),
 actor_id TEXT NOT NULL, action TEXT NOT NULL, detail JSONB NOT NULL DEFAULT '{}',
 created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS four_graves_test_tickets (
 event_key TEXT NOT NULL CHECK(event_key LIKE 'four-graves-private-test-%'),
 guild_id TEXT NOT NULL, discord_id TEXT NOT NULL,
 treats INTEGER NOT NULL CHECK(treats>=0), tricks INTEGER NOT NULL CHECK(tricks>=0),
 PRIMARY KEY(event_key,guild_id,discord_id)
);

-- One immutable, administrator-triggered prize draw per event.
-- Separate from legacy role/channel/decision fields retained for compatibility.
CREATE TABLE IF NOT EXISTS four_graves_draws (
 event_id BIGINT PRIMARY KEY REFERENCES four_graves_events(id),
 snapshot_hash TEXT NOT NULL,
 results JSONB NOT NULL,
 results_hash TEXT NOT NULL,
 drawn_by TEXT NOT NULL,
 drawn_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
 channel_id TEXT,
 message_id TEXT,
 published_at TIMESTAMPTZ
);
