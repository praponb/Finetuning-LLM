WITH filtered AS (
    SELECT
        id,
        customer_id,
        browser,
        started_at,
        country,
        QUARTER(started_at) AS started_at_quarter,
        SUBSTRING(country, 1, 2) AS country_prefix,
        COALESCE(country, 'DIRECT') AS country_clean
    FROM sessions
    WHERE started_at >= TIMESTAMP '2023-06-28 00:00:00'
      AND country NOT IN ('SG', 'UK', 'US')
),
enriched AS (
    SELECT
        *,
        MAX(started_at_quarter) OVER (PARTITION BY country) AS country_max_started_at_quarter,
        ROW_NUMBER() OVER (ORDER BY started_at_quarter DESC, id) AS seq_no
    FROM filtered
),
with_windows AS (
    SELECT
        id,
        started_at,
        country_prefix,
        country_clean,
        country_max_started_at_quarter,
        seq_no,
        AVG(started_at_quarter) OVER (PARTITION BY browser ORDER BY country_max_started_at_quarter, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS started_at_quarter_avg_5,
        SUM(country_max_started_at_quarter) OVER (PARTITION BY country ORDER BY started_at_quarter, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_country_max_started_at_quarter,
        RANK() OVER (ORDER BY started_at_quarter DESC NULLS FIRST) AS started_at_quarter_rank,
        SUM(country_max_started_at_quarter) OVER (PARTITION BY browser ORDER BY CAST(started_at AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS country_max_started_at_quarter_sum_30d,
        MAX(country_max_started_at_quarter) OVER (PARTITION BY customer_id, country ORDER BY CAST(started_at AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS country_max_started_at_quarter_max_7d
    FROM enriched
),
base AS (
    SELECT *, COALESCE(country_max_started_at_quarter_sum_30d, 0) AS country_max_started_at_quarter_sum_30d_filled
    FROM with_windows
    WHERE started_at_quarter_rank >= 5
),
with_last AS (
    SELECT
        s.id,
        s.country_clean,
        s.country_max_started_at_quarter,
        s.seq_no,
        top1.web_event_event_time
    FROM base s
    LEFT JOIN LATERAL (
            SELECT c.event_time AS web_event_event_time
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.duration_sec DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_windows_sessions AS (
    SELECT
        *,
        FIRST_VALUE(seq_no) IGNORE NULLS OVER (ORDER BY country_max_started_at_quarter, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_seq_no,
        SUM(country_max_started_at_quarter) OVER (ORDER BY web_event_event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_country_max_started_at_quarter,
        AVG(seq_no) OVER (ORDER BY web_event_event_time, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS seq_no_avg_2
    FROM with_last
),
with_windows_sessions_2 AS (
    SELECT
        *,
        LAG(first_known_seq_no) OVER (ORDER BY seq_no_avg_2, id) AS prev_first_known_seq_no,
        LEAD(seq_no_avg_2) OVER (ORDER BY seq_no_avg_2, id) AS next_seq_no_avg_2,
        LAG(seq_no_avg_2) OVER (ORDER BY seq_no_avg_2, id) AS prev_seq_no_avg_2,
        ROW_NUMBER() OVER (ORDER BY seq_no_avg_2, id) AS event_seq,
        RANK() OVER (ORDER BY seq_no_avg_2 DESC) AS seq_no_avg_2_rank
    FROM with_windows_sessions
),
stats AS (
    SELECT
        CASE WHEN seq_no_avg_2_rank >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END AS seq_no_avg_2_rank_tier,
        DATE_TRUNC('WEEK', web_event_event_time) AS week_start,
        ROUND(AVG(seq_no), 2) AS avg_seq_no,
        COUNT(*) AS row_count,
        APPROX_PERCENTILE(first_known_seq_no, 0.95) AS first_known_seq_no_p95,
        COUNT(DISTINCT id) AS distinct_ids
    FROM with_windows_sessions_2
    GROUP BY seq_no_avg_2_rank_tier, week_start
    HAVING COUNT(*) < 1000
)
SELECT
    seq_no_avg_2_rank_tier,
    week_start,
    row_count,
    first_known_seq_no_p95
FROM stats
ORDER BY row_count DESC NULLS FIRST, first_known_seq_no_p95 DESC;
