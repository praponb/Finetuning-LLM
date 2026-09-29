WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        level,
        latency_ms,
        status_code,
        attr_key,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS attr_key, v
),
parsed AS (
    -- Struct fields, map lookups and JSON extraction
    SELECT
        id,
        host,
        attr_key,
        v,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 8) != 0 AS INT) AS flag_bit8
    FROM lines
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY attr_key, host ORDER BY flag_bit8 DESC NULLS LAST, id, attr_key) AS latest_rank
        FROM parsed
    ) t
    WHERE latest_rank = 1
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        attr_key,
        status_code_high,
        flag_bit8,
        latest_rank,
        COUNT(flag_bit8) OVER w_ordered AS last4_count_flag_bit8,
        MAX(flag_bit8) OVER w_ordered AS last4_max_flag_bit8,
        SUM(flag_bit8) OVER w_ordered AS last4_sum_flag_bit8,
        NTILE(5) OVER w_ordered_2 AS flag_bit8_bucket
    FROM latest
    WINDOW w_ordered AS (PARTITION BY attr_key, host ORDER BY flag_bit8, id, attr_key ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), w_ordered_2 AS (PARTITION BY attr_key ORDER BY flag_bit8 DESC, id, attr_key)
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY attr_key ORDER BY latest_rank DESC, id, attr_key) AS dedup_rank
    FROM enriched
    QUALIFY dedup_rank = 1
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        attr_key,
        status_code_high,
        latest_rank,
        last4_max_flag_bit8,
        last4_sum_flag_bit8,
        MIN(status_code_high) OVER (PARTITION BY attr_key ORDER BY flag_bit8, id, attr_key ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_min_status_code_high,
        COUNT(status_code_high) OVER (PARTITION BY attr_key ORDER BY flag_bit8, id, attr_key ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_count_status_code_high,
        AVG(status_code_high) OVER (PARTITION BY attr_key ORDER BY flag_bit8, id, attr_key ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_avg_status_code_high,
        NTILE(5) OVER (PARTITION BY attr_key ORDER BY last4_count_flag_bit8 DESC, id, attr_key) AS last4_count_flag_bit8_bucket
    FROM deduped
),
latest_app_logs AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY attr_key ORDER BY last4_count_flag_bit8_bucket DESC NULLS FIRST, id, attr_key) AS latest_rank_2
    FROM metrics
    QUALIFY latest_rank_2 = 1
)
SELECT
    id,
    attr_key,
    latest_rank,
    last4_max_flag_bit8,
    last4_sum_flag_bit8,
    last3_count_status_code_high,
    last3_avg_status_code_high,
    CASE WHEN attr_key RLIKE '^(version|region)$' THEN 'GROUP_B' ELSE 'EXTENDED' END AS attr_key_zone
FROM latest_app_logs
ORDER BY last3_count_status_code_high DESC
