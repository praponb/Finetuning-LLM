WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        level,
        status_code,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) t AS attr_key, attr_value
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        ROUND(status_code / SUM(status_code) OVER (PARTITION BY attr_key, level), 4) status_code_share,
        PERCENT_RANK() OVER (PARTITION BY attr_key, level ORDER BY status_code ASC) status_code_percent_rank,
        FIRST_VALUE(attr_key) IGNORE NULLS OVER (PARTITION BY attr_key, level ORDER BY status_code ASC, id ASC, attr_key ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_attr_key,
        STDDEV_SAMP(status_code) OVER (PARTITION BY attr_key) attr_key_stddev_samp_status_code
    FROM lines
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(status_code_share) OVER (PARTITION BY level ORDER BY status_code_percent_rank ASC, id ASC, attr_key ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) status_code_share_avg_3,
        SUM(status_code_percent_rank) OVER (ORDER BY status_code_share ASC, id ASC, attr_key ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_status_code_percent_rank
    FROM with_windows
    WHERE status_code_percent_rank > 0.47
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        level,
        attr_key,
        attr_value,
        status_code_share,
        attr_key_stddev_samp_status_code,
        status_code_share_avg_3,
        cumulative_status_code_percent_rank,
        LEAD(status_code_share_avg_3, 2, 0) OVER (PARTITION BY attr_key, level ORDER BY status_code_share_avg_3 ASC, id ASC, attr_key ASC) next_status_code_share_avg_3,
        AVG(status_code_share) OVER w_recent status_code_share_avg_2
    FROM metrics
    WINDOW w_recent AS (PARTITION BY attr_key, level ORDER BY status_code_percent_rank ASC, id ASC, attr_key ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY attr_key ORDER BY status_code_share_avg_2 ASC, id ASC, attr_key ASC) AS latest_rank
    FROM enriched
    QUALIFY latest_rank = 1
)
-- Grouped metrics
SELECT
    level,
    ROUND(SUM(next_status_code_share_avg_3) / NULLIF(COUNT(CASE WHEN level = 'INFO' THEN 1 END), 0), 2) AS next_status_code_share_avg_3_per_info,
    APPROX_PERCENTILE(status_code_share_avg_3, 0.5) AS status_code_share_avg_3_p50,
    ROUND(AVG(status_code_share_avg_2), 2) AS avg_status_code_share_avg_2,
    MAX(status_code_share_avg_2) FILTER (WHERE attr_key = 'region') AS region_max_status_code_share_avg_2,
    ROUND(AVG(status_code_share), 2) AS avg_status_code_share
FROM ranked
GROUP BY level
ORDER BY status_code_share_avg_3_p50 ASC NULLS LAST
