WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LOWER(country) AS country_lower
    FROM sessions
    WHERE started_at > DATE '2023-08-06'
      AND customer_id IS NOT NULL
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY device ORDER BY started_at DESC NULLS LAST, id) AS latest_rank
    FROM valid
    QUALIFY latest_rank = 1
),
deduped_sessions AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        started_at,
        country,
        latest_rank
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY device ORDER BY latest_rank DESC, id) AS row_num
        FROM deduped
    ) ranked
    WHERE row_num = 1
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY latest_rank DESC, id) AS row_seq,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY latest_rank, id) AS event_seq
    FROM deduped_sessions
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN started_at - LAG(started_at) OVER (PARTITION BY country ORDER BY started_at, id) > INTERVAL '14' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM with_windows
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY country ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
)
SELECT
    id,
    started_at,
    country,
    latest_rank,
    row_seq,
    event_seq,
    is_new_session,
    session_no
FROM sessionized
ORDER BY row_seq DESC
