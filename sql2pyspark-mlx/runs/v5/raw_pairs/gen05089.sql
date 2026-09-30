WITH filtered AS (
    SELECT
        id,
        session_id,
        event_type,
        page,
        ROUND(duration_sec, 0) AS duration_sec_rounded
    FROM web_events
    WHERE page IS NOT NULL
      AND page IS DISTINCT FROM '/checkout'
),
deduped AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY duration_sec_rounded, id) AS rank_in_group
        FROM filtered
    ) x
    WHERE rank_in_group = 1
),
deduped_web_events AS (
    SELECT *
    FROM deduped
    QUALIFY ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY rank_in_group, id) = 1
),
with_windows AS (
    SELECT
        id,
        duration_sec_rounded,
        LAST_VALUE(page) IGNORE NULLS OVER (PARTITION BY event_type, page ORDER BY duration_sec_rounded, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_page,
        LEAD(duration_sec_rounded, 1, 0) OVER (PARTITION BY session_id ORDER BY duration_sec_rounded, id) AS next_duration_sec_rounded
    FROM deduped_web_events
),
metrics AS (
    SELECT
        id,
        duration_sec_rounded,
        last_known_page,
        AVG(next_duration_sec_rounded) OVER (ORDER BY next_duration_sec_rounded, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS next_duration_sec_rounded_avg_2_prior,
        DENSE_RANK() OVER w_ordered AS duration_sec_rounded_dense_rank
    FROM with_windows
    WINDOW w_ordered AS (ORDER BY duration_sec_rounded DESC NULLS LAST)
),
stats AS (
    SELECT
        CASE WHEN duration_sec_rounded >= 67 THEN 'LARGE' ELSE 'SMALL' END AS duration_sec_rounded_tier,
        CASE WHEN next_duration_sec_rounded_avg_2_prior >= 78 THEN 'LARGE' ELSE 'SMALL' END AS next_duration_sec_rounded_avg_2_prior_tier,
        COUNT(DISTINCT id) AS distinct_ids,
        ROUND(AVG(duration_sec_rounded), 2) AS avg_duration_sec_rounded,
        ROUND(AVG(next_duration_sec_rounded_avg_2_prior), 2) AS avg_next_duration_sec_rounded_avg_2_prior,
        PERCENT_RANK() OVER (PARTITION BY CASE WHEN duration_sec_rounded >= 67 THEN 'LARGE' ELSE 'SMALL' END ORDER BY COUNT(DISTINCT id) DESC) AS distinct_ids_percent_rank
    FROM metrics
    GROUP BY CASE WHEN duration_sec_rounded >= 67 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN next_duration_sec_rounded_avg_2_prior >= 78 THEN 'LARGE' ELSE 'SMALL' END
    HAVING ROUND(AVG(next_duration_sec_rounded_avg_2_prior), 2) < 1000000
)
SELECT
    duration_sec_rounded_tier,
    next_duration_sec_rounded_avg_2_prior_tier,
    distinct_ids,
    avg_duration_sec_rounded,
    avg_next_duration_sec_rounded_avg_2_prior
FROM stats
ORDER BY next_duration_sec_rounded_avg_2_prior_tier NULLS LAST, avg_next_duration_sec_rounded_avg_2_prior
