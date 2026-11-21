-- Audit log of detections and mitigation actions. Unique keys make replayed scores no-ops.

CREATE TABLE alerts (
    src_ip      inet        NOT NULL,
    window_end  bigint      NOT NULL,
    score       real        NOT NULL,
    detector    text        NOT NULL,
    ts          timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (src_ip, window_end)
);

CREATE TABLE actions (
    id          bigserial   PRIMARY KEY,
    ts          timestamptz NOT NULL DEFAULT now(),
    src_ip      inet        NOT NULL,
    action      text        NOT NULL CHECK (action IN ('rate_limit', 'block')),
    reason      text        NOT NULL,
    detector    text        NOT NULL,
    score       real        NOT NULL,
    window_end  bigint      NOT NULL,     -- the score window that triggered it
    expires_at  timestamptz NOT NULL,
    expired_at  timestamptz,              -- set when the expiry is recorded
    UNIQUE (src_ip, action, window_end)
);

CREATE INDEX actions_by_source ON actions (src_ip, expires_at);
