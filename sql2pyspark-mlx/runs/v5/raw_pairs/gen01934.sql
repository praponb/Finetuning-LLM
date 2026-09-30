WITH joined AS (
    -- Attach sessions attributes
    SELECT
        a.*,
        b.browser,
        b.device
    FROM web_events a
    LEFT JOIN sessions b
        ON a.session_id = b.id
    WHERE b.country IN ('JP', 'TH')
)
-- Multi-level aggregation
SELECT
    CASE WHEN duration_sec >= 403 THEN 'LARGE' ELSE 'SMALL' END AS duration_sec_tier,
    BIT_AND(duration_sec) AS duration_sec_bit_and,
    BIT_OR(duration_sec) AS duration_sec_bit_or,
    MAX(duration_sec) FILTER (WHERE event_type = 'purchase') AS purchase_max_duration_sec,
    COUNT(DISTINCT id) AS distinct_ids
FROM joined
GROUP BY CASE WHEN duration_sec >= 403 THEN 'LARGE' ELSE 'SMALL' END
