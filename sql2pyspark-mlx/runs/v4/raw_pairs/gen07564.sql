WITH deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY duration_sec DESC NULLS FIRST, id) AS rn
        FROM web_events
    ) x
    WHERE rn = 1
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(duration_sec) OVER (ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS duration_sec_avg_30d,
        NTILE(5) OVER (PARTITION BY page ORDER BY duration_sec DESC, id) AS duration_sec_bucket,
        AVG(duration_sec) OVER w_running AS duration_sec_avg_2_prior,
        MAX(duration_sec) OVER (PARTITION BY event_type ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS duration_sec_max_7d,
        RANK() OVER (PARTITION BY session_id ORDER BY duration_sec DESC NULLS LAST) AS duration_sec_rank
    FROM deduped
    WINDOW w_running AS (ORDER BY rn, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING)
),
deduped_web_events AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM scored
    QUALIFY RANK() OVER (PARTITION BY page ORDER BY duration_sec_avg_2_prior DESC) = 1
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN event_time - LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time, id) > INTERVAL '30' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM deduped_web_events
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY event_type ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
)
-- Multi-level aggregation
SELECT
    CASE WHEN duration_sec_avg_30d >= 362 THEN 'LARGE' ELSE 'SMALL' END AS duration_sec_avg_30d_tier,
    CASE WHEN duration_sec_avg_2_prior >= 374 THEN 'LARGE' ELSE 'SMALL' END AS duration_sec_avg_2_prior_tier,
    COUNT(CASE WHEN page = '/search' THEN 1 END) AS search_count,
    MIN(event_time) AS first_event_time,
    SUM(CASE WHEN event_type = 'purchase' THEN duration_sec ELSE 0 END) AS purchase_duration_sec,
    SORT_ARRAY(COLLECT_SET(event_type)) AS event_type_list,
    PERCENTILE_APPROX(island_id, ARRAY(0.25, 0.5, 0.75)) AS island_id_quartiles
FROM sessionized
GROUP BY CASE WHEN duration_sec_avg_30d >= 362 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN duration_sec_avg_2_prior >= 374 THEN 'LARGE' ELSE 'SMALL' END
HAVING SUM(CASE WHEN event_type = 'purchase' THEN duration_sec ELSE 0 END) < 0
