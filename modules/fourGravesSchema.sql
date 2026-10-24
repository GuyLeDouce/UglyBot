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

-- Per-grave durable 2-minute wait, 15-second draw pacing, and recovery markers.
ALTER TABLE four_graves_graves
 ADD COLUMN IF NOT EXISTS choice_message_id TEXT,
 ADD COLUMN IF NOT EXISTS next_draw_at TIMESTAMPTZ,
 ADD COLUMN IF NOT EXISTS draw_completed_at TIMESTAMPTZ;

-- One immutable winning ticket per prize; publication is reconciled by marker.
CREATE TABLE IF NOT EXISTS four_graves_awards (
 event_id BIGINT NOT NULL,
 grave_id TEXT NOT NULL,
 prize_index INTEGER NOT NULL CHECK (prize_index > 0),
 prize_name TEXT NOT NULL,
 prize_unit INTEGER NOT NULL CHECK (prize_unit > 0),
 winner_discord_id TEXT NOT NULL,
 winner_display_name TEXT NOT NULL,
 ticket_entry_id TEXT NOT NULL,
 pass_number INTEGER NOT NULL CHECK (pass_number > 0),
 message_id TEXT,
 announced_at TIMESTAMPTZ,
 PRIMARY KEY(event_id,grave_id,prize_index),
 FOREIGN KEY(event_id,grave_id) REFERENCES four_graves_graves(event_id,grave_id)
);

-- Four separate, persistent roster messages beneath the selection panel.
-- Per-grave UI metadata only: tickets, allocations, winners and pools unchanged.
ALTER TABLE four_graves_graves
 ADD COLUMN IF NOT EXISTS roster_message_id TEXT;

-- A single final public recap is posted only after every populated grave
-- announces every prize. Each message has a stable page index and footer
-- marker; recovery adopts existing pages and never posts duplicate winners.
ALTER TABLE four_graves_events
 ADD COLUMN IF NOT EXISTS rewards_download_message_id TEXT,
 ADD COLUMN IF NOT EXISTS rewards_published_at TIMESTAMPTZ;

CREATE TABLE IF NOT EXISTS four_graves_reward_summary_pages (
 event_id BIGINT NOT NULL REFERENCES four_graves_events(id),
 page_index INTEGER NOT NULL CHECK (page_index >= 0),
 message_id TEXT,
 PRIMARY KEY(event_id, page_index)
);
