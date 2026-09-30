WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        status_code,
        key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS key, value
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        host,
        status_code,
        key,
        value,
        FIRST_VALUE(key, TRUE) OVER (PARTITION BY host ORDER BY status_code, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_key,
        NTILE(4) OVER (PARTITION BY host ORDER BY status_code, id, key) status_code_bucket,
        SUM(status_code) OVER (ORDER BY status_code, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_status_code,
        NTILE(3) OVER (PARTITION BY host, key ORDER BY status_code DESC, id, key) status_code_bucket_2,
        NTILE(10) OVER (PARTITION BY key ORDER BY status_code DESC, id, key) status_code_bucket_3
    FROM flattened
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *, RANK() OVER (PARTITION BY host ORDER BY status_code_bucket_2) AS dedup_rank
    FROM enriched
    QUALIFY dedup_rank = 1
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY key, host ORDER BY status_code_bucket_3 DESC) AS dedup_rank_2
        FROM ranked
    ) t
    WHERE dedup_rank_2 = 1
)
SELECT
    id,
    host,
    status_code,
    key,
    first_known_key,
    status_code_bucket,
    cumulative_status_code,
    status_code_bucket_2,
    status_code_bucket_3
FROM latest
ORDER BY cumulative_status_code DESC, host;
