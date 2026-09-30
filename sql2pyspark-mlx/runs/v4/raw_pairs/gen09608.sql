WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        status_code,
        attr_key,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS attr_key, v
),
kept_keys AS (
    -- Set difference on keys
    SELECT id
    FROM lines
    EXCEPT DISTINCT
    SELECT id
    FROM app_logs
    WHERE level = 'INFO'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM lines s
    INNER JOIN kept_keys k ON s.id = k.id
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROUND(status_code / SUM(status_code) OVER (PARTITION BY attr_key), 4) AS status_code_share,
        SUM(status_code) OVER (PARTITION BY attr_key) AS attr_key_sum_status_code
    FROM kept_rows
),
metrics_app_logs AS (
    -- Ranking and gap features
    SELECT
        id,
        status_code,
        attr_key,
        v,
        status_code_share,
        attr_key_sum_status_code,
        PERCENT_RANK() OVER (ORDER BY status_code_share DESC NULLS FIRST) AS status_code_share_percent_rank,
        RANK() OVER (ORDER BY attr_key_sum_status_code DESC) AS attr_key_sum_status_code_rank
    FROM metrics
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        status_code,
        attr_key,
        v,
        status_code_share,
        attr_key_sum_status_code,
        attr_key_sum_status_code_rank,
        GREATEST(attr_key_sum_status_code, status_code_share) AS greatest_attr_key_sum_status_code_status_code_share,
        CAST(status_code AS DOUBLE) AS status_code_num
    FROM metrics_app_logs
    WHERE attr_key IS NOT NULL
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        status_code,
        attr_key,
        v,
        status_code_share,
        attr_key_sum_status_code,
        attr_key_sum_status_code_rank,
        greatest_attr_key_sum_status_code_status_code_share,
        status_code_num,
        ROUND(status_code / SUM(status_code) OVER (PARTITION BY attr_key), 4) AS status_code_share_2,
        ROUND(attr_key_sum_status_code / SUM(attr_key_sum_status_code) OVER (PARTITION BY attr_key), 4) AS attr_key_sum_status_code_share,
        COUNT(attr_key_sum_status_code_rank) OVER (PARTITION BY attr_key) AS attr_key_count_attr_key_sum_status_code_rank
    FROM cleaned
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(status_code_share_2) OVER w_recent AS attr_key_sum_status_code_share_2,
        COUNT(attr_key_sum_status_code_share) OVER w_recent AS attr_key_count_attr_key_sum_status_code_share
    FROM scored
    WINDOW w_recent AS (PARTITION BY attr_key)
)
SELECT
    id,
    status_code,
    attr_key_sum_status_code,
    attr_key_sum_status_code_rank,
    status_code_num,
    status_code_share_2,
    attr_key_sum_status_code_share,
    attr_key_count_attr_key_sum_status_code_rank,
    attr_key_sum_status_code_share_2
FROM with_windows
ORDER BY attr_key_sum_status_code_share_2 DESC;
