WITH ranked AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        session_id,
        event_type,
        duration_sec
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY page ORDER BY duration_sec DESC NULLS LAST, id) AS rn
        FROM web_events
    ) x
    WHERE rn = 1
),
with_dims AS (
    -- Attach sessions attributes
    SELECT
        s.id,
        s.session_id,
        b.browser,
        b.country,
        b.device
    FROM ranked s
    LEFT JOIN sessions b
        ON s.session_id = b.id
    WHERE b.country IN ('TH', 'DE', 'US')
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        session_id,
        browser,
        device,
        CASE WHEN country = 'JP' THEN 1 ELSE 0 END AS is_jp,
        COALESCE(country, 'UNKNOWN') AS country_clean
    FROM with_dims
    WHERE country = 'SG'
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(is_jp, 1) OVER w_grp AS prev_is_jp,
        AVG(is_jp) OVER w_ordered AS is_jp_avg_3_prior,
        AVG(is_jp) OVER (PARTITION BY device, session_id ORDER BY is_jp, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS is_jp_avg_5_prior,
        AVG(is_jp) OVER (PARTITION BY browser ORDER BY is_jp, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS is_jp_avg_2
    FROM base
    WINDOW w_grp AS (PARTITION BY browser ORDER BY is_jp, id), w_ordered AS (PARTITION BY session_id ORDER BY is_jp, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING)
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(is_jp) OVER w_running AS next_is_jp,
        LEAD(is_jp_avg_3_prior) OVER w_running AS next_is_jp_avg_3_prior,
        ROW_NUMBER() OVER w_running AS row_seq,
        NTILE(4) OVER w_recent AS prev_is_jp_bucket
    FROM scored
    WINDOW w_running AS (PARTITION BY device ORDER BY prev_is_jp, id), w_recent AS (PARTITION BY browser ORDER BY prev_is_jp DESC, id)
)
SELECT
    id,
    session_id,
    is_jp,
    country_clean,
    is_jp_avg_3_prior,
    is_jp_avg_2,
    next_is_jp,
    next_is_jp_avg_3_prior,
    row_seq
FROM metrics
ORDER BY session_id DESC
