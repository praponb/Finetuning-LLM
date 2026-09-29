WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        level,
        latency_ms,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS attr_key, attr_value
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id, service
    FROM lines
    INTERSECT
    SELECT id, service
    FROM app_logs
    WHERE level = 'ERROR'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM lines s
    INNER JOIN matched_keys k ON s.id = k.id AND s.service = k.service
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        STDDEV_SAMP(latency_ms) OVER w_part attr_key_stddev_samp_latency_ms,
        PERCENT_RANK() OVER w_running latency_ms_percent_rank
    FROM kept_rows
    WINDOW w_part AS (PARTITION BY attr_key), w_running AS (PARTITION BY level ORDER BY latency_ms DESC NULLS FIRST)
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CAST(latency_ms_percent_rank AS INT) latency_ms_percent_rank_int,
        TRIM(attr_key) clean_attr_key
    FROM enriched
    WHERE attr_key IS NOT NULL
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CAST(attr_key_stddev_samp_latency_ms AS STRING) attr_key_stddev_samp_latency_ms_num,
        SUBSTRING(attr_key, 1, 2) attr_key_prefix
    FROM filtered
    WHERE attr_key IS NOT NULL
      AND latency_ms_percent_rank > 0.27
),
base_app_logs AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        level,
        latency_ms,
        attr_key,
        attr_value,
        attr_key_stddev_samp_latency_ms,
        latency_ms_percent_rank,
        clean_attr_key,
        attr_key_stddev_samp_latency_ms_num,
        attr_key_prefix,
        GREATEST(latency_ms_percent_rank_int, latency_ms_percent_rank) greatest_latency_ms_percent_rank_int_latency_ms_percent_rank,
        COALESCE(attr_key, 'UNKNOWN') resolved_attr_key
    FROM base
    WHERE attr_key = 'trace'
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY attr_key, level ORDER BY attr_key_stddev_samp_latency_ms) attr_key_stddev_samp_latency_ms_cume_dist,
        DENSE_RANK() OVER w_part latency_ms_percent_rank_dense_rank,
        RANK() OVER (ORDER BY greatest_latency_ms_percent_rank_int_latency_ms_percent_rank DESC NULLS FIRST) greatest_latency_ms_percent_rank_int_latency_ms_percent_rank_rank,
        DENSE_RANK() OVER (ORDER BY attr_key_stddev_samp_latency_ms NULLS LAST) attr_key_stddev_samp_latency_ms_dense_rank,
        MIN(latency_ms_percent_rank) OVER (PARTITION BY attr_key) attr_key_min_latency_ms_percent_rank
    FROM base_app_logs
    WINDOW w_part AS (PARTITION BY attr_key, level ORDER BY latency_ms_percent_rank DESC)
)
SELECT
    id,
    level,
    latency_ms,
    attr_value,
    attr_key_stddev_samp_latency_ms,
    latency_ms_percent_rank,
    clean_attr_key,
    attr_key_prefix,
    attr_key_stddev_samp_latency_ms_cume_dist,
    CASE WHEN level RLIKE '^(INFO|WARN)$' THEN 'EXTENDED' ELSE 'PRIORITY' END AS level_zone
FROM with_windows
ORDER BY latency_ms NULLS FIRST;
