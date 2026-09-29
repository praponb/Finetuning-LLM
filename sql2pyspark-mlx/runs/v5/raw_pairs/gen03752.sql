WITH exploded AS (
    SELECT
        id,
        status_code,
        attr_key,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS attr_key, v
),
flattened_attrs AS (
    SELECT
        id,
        attr_key,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 8) != 0 AS INT) AS is_flagged
    FROM exploded
),
metrics AS (
    SELECT
        id,
        attr_key,
        RANK() OVER w_part AS status_code_high_rank,
        NTILE(4) OVER w_grp AS is_flagged_bucket
    FROM flattened_attrs
    WINDOW w_part AS (PARTITION BY attr_key ORDER BY status_code_high DESC NULLS FIRST), w_grp AS (ORDER BY is_flagged DESC, id, attr_key)
),
deduped AS (
    SELECT
        id,
        attr_key,
        status_code_high_rank,
        is_flagged_bucket
    FROM metrics
    QUALIFY ROW_NUMBER() OVER (PARTITION BY attr_key ORDER BY status_code_high_rank DESC NULLS LAST, id, attr_key) = 1
),
scored AS (
    SELECT
        id,
        attr_key,
        status_code_high_rank,
        is_flagged_bucket,
        NTILE(3) OVER (PARTITION BY attr_key ORDER BY is_flagged_bucket DESC, id, attr_key) AS is_flagged_bucket_bucket,
        ROW_NUMBER() OVER (PARTITION BY attr_key ORDER BY is_flagged_bucket, id, attr_key) AS event_seq
    FROM deduped
)
SELECT
    attr_key,
    ROUND(SUM(is_flagged_bucket_bucket) / NULLIF(COUNT(CASE WHEN attr_key = 'region' THEN 1 END), 0), 2) AS is_flagged_bucket_bucket_per_region,
    COUNT(*) FILTER (WHERE attr_key = 'version') AS version_rows
FROM scored
GROUP BY attr_key
