-- Additive schema only. Executed in one transaction under a feature advisory lock.
CREATE TABLE IF NOT EXISTS madlib_schema (singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK(singleton), version INTEGER NOT NULL);
INSERT INTO madlib_schema VALUES(TRUE,1) ON CONFLICT DO NOTHING;
CREATE TABLE IF NOT EXISTS madlib_guilds (
  guild_id TEXT PRIMARY KEY, finances_paused BOOLEAN NOT NULL DEFAULT FALSE,
  revision INTEGER NOT NULL DEFAULT 0, updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS madlib_users (
  guild_id TEXT NOT NULL, user_id TEXT NOT NULL, next_free_at TIMESTAMPTZ NOT NULL DEFAULT '1970-01-01',
  active_session_id TEXT, PRIMARY KEY(guild_id,user_id)
);
CREATE TABLE IF NOT EXISTS madlib_sessions (
  id TEXT PRIMARY KEY, guild_id TEXT NOT NULL, user_id TEXT NOT NULL, display_name TEXT NOT NULL,
  template_id TEXT NOT NULL, template_version INTEGER NOT NULL, template JSONB NOT NULL,
  answers JSONB NOT NULL DEFAULT '{}', step INTEGER NOT NULL DEFAULT 0 CHECK(step>=0), revision INTEGER NOT NULL DEFAULT 0,
  state TEXT NOT NULL CHECK(state IN ('active','awaiting_payment','payment_failed','payment_review','completed','cancelled','failed','refund_pending','refunded')),
  cost INTEGER NOT NULL DEFAULT 0 CHECK(cost>=0), previous_free_at TIMESTAMPTZ, reserved_free_until TIMESTAMPTZ,
  story TEXT, prompt TEXT, delivered_at TIMESTAMPTZ, delivery_attempts INTEGER NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(), completed_at TIMESTAMPTZ, updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  FOREIGN KEY(guild_id,user_id) REFERENCES madlib_users(guild_id,user_id)
);
CREATE UNIQUE INDEX IF NOT EXISTS madlib_one_active ON madlib_sessions(guild_id,user_id)
  WHERE state IN ('active','awaiting_payment','payment_failed','payment_review','refund_pending');
CREATE INDEX IF NOT EXISTS madlib_history ON madlib_sessions(guild_id,user_id,completed_at DESC,id) WHERE state='completed';
CREATE TABLE IF NOT EXISTS madlib_quotes (
  id TEXT PRIMARY KEY, guild_id TEXT NOT NULL, user_id TEXT NOT NULL, amount INTEGER NOT NULL CHECK(amount>0),
  expires_at TIMESTAMPTZ NOT NULL, UNIQUE(guild_id,user_id)
);
CREATE TABLE IF NOT EXISTS madlib_panels (
  id TEXT PRIMARY KEY, guild_id TEXT NOT NULL, channel_id TEXT NOT NULL, message_id TEXT UNIQUE, creator_id TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS madlib_drafts (
  guild_id TEXT NOT NULL, user_id TEXT NOT NULL, session_id TEXT NOT NULL REFERENCES madlib_sessions(id),
  revision INTEGER NOT NULL DEFAULT 0, updated_at TIMESTAMPTZ NOT NULL DEFAULT now(), PRIMARY KEY(guild_id,user_id)
);
CREATE TABLE IF NOT EXISTS madlib_uploads (
  session_id TEXT PRIMARY KEY REFERENCES madlib_sessions(id), bytes BYTEA NOT NULL CHECK(octet_length(bytes)<=8388608),
  media_type TEXT NOT NULL, width INTEGER NOT NULL, height INTEGER NOT NULL, revision INTEGER NOT NULL,
  expires_at TIMESTAMPTZ NOT NULL DEFAULT now()+interval '24 hours'
);
CREATE TABLE IF NOT EXISTS madlib_publications (
  id TEXT PRIMARY KEY, session_id TEXT NOT NULL UNIQUE REFERENCES madlib_sessions(id), guild_id TEXT NOT NULL,
  user_id TEXT NOT NULL, channel_id TEXT NOT NULL, message_id TEXT UNIQUE, attachment_id TEXT, filename TEXT NOT NULL,
  upload_revision INTEGER NOT NULL,
  status TEXT NOT NULL CHECK(status IN ('prepared','publishing','needs_review','published','deleted')),
  emoji_id TEXT NOT NULL, realm_id TEXT NOT NULL, currency_id TEXT NOT NULL, reward INTEGER NOT NULL CHECK(reward>0),
  reward_cap INTEGER NOT NULL DEFAULT 0 CHECK(reward_cap>=0), reactor_roles JSONB NOT NULL DEFAULT '[]',
  suspended BOOLEAN NOT NULL DEFAULT FALSE, moderation_reason TEXT, revision INTEGER NOT NULL DEFAULT 0,
  lease_id TEXT, lease_until TIMESTAMPTZ, emoji_seeded BOOLEAN NOT NULL DEFAULT FALSE,
  reconcile_after TEXT, reconcile_type INTEGER NOT NULL DEFAULT 0 CHECK(reconcile_type IN (0,1)),
  next_reconcile_at TIMESTAMPTZ NOT NULL DEFAULT now(), scan_before TEXT,
  display_dirty BOOLEAN NOT NULL DEFAULT TRUE, last_display_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS madlib_publication_jobs ON madlib_publications(next_reconcile_at) WHERE status IN ('published','needs_review');
CREATE TABLE IF NOT EXISTS madlib_operations (
  id TEXT PRIMARY KEY, guild_id TEXT NOT NULL, user_id TEXT NOT NULL,
  kind TEXT NOT NULL CHECK(kind IN ('debit','reward','refund')), amount INTEGER NOT NULL CHECK(amount>0),
  realm_id TEXT NOT NULL, currency_id TEXT NOT NULL,
  session_id TEXT REFERENCES madlib_sessions(id), publication_id TEXT REFERENCES madlib_publications(id), reactor_id TEXT,
  sender_id TEXT, recipient_id TEXT,
  state TEXT NOT NULL DEFAULT 'prepared' CHECK(state IN ('prepared','resolving','in_flight','confirmed_success','confirmed_failure','retryable_failure','needs_review')),
  attempt_count INTEGER NOT NULL DEFAULT 0, revision INTEGER NOT NULL DEFAULT 0,
  lease_id TEXT, lease_until TIMESTAMPTZ, error_code TEXT, provider_ref TEXT,
  next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(), sent_at TIMESTAMPTZ, confirmed_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS madlib_financial_jobs ON madlib_operations(next_attempt_at,created_at) WHERE state IN ('prepared','retryable_failure');
CREATE TABLE IF NOT EXISTS madlib_reactions (
  publication_id TEXT NOT NULL REFERENCES madlib_publications(id), reactor_id TEXT NOT NULL, emoji_id TEXT NOT NULL,
  amount INTEGER NOT NULL CHECK(amount>0), realm_id TEXT NOT NULL, currency_id TEXT NOT NULL,
  operation_id TEXT NOT NULL UNIQUE REFERENCES madlib_operations(id), created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY(publication_id,reactor_id,emoji_id)
);
CREATE TABLE IF NOT EXISTS madlib_audit (
  id BIGSERIAL PRIMARY KEY, guild_id TEXT NOT NULL, actor_id TEXT NOT NULL, action TEXT NOT NULL,
  record_id TEXT, evidence TEXT NOT NULL, detail JSONB NOT NULL DEFAULT '{}', created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
