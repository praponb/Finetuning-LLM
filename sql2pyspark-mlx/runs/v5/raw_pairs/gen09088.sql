WITH lines AS (
    SELECT
        id,
        service,
        latency_ms,
        status_code,
        key,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) t AS key, v
),
parsed AS (
    SELECT
        id,
        status_code,
        key,
        CAST((status_code & 2) != 0 AS INT) AS is_flagged,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM lines
),
cleaned AS (
    SELECT
        *,
        CASE WHEN key IN ('trace', 'region') THEN 'Group A' ELSE 'Other' END AS key_group,
        is_flagged + status_code AS is_flagged_plus_status_code,
        COALESCE(key, 'UNKNOWN') AS key_clean
    FROM parsed
    WHERE status_code_high BETWEEN 1 AND 2
),
deduped AS (
    SELECT
        id,
        key,
        is_flagged,
        is_flagged_plus_status_code,
        key_clean
    FROM cleaned
    QUALIFY ROW_NUMBER() OVER (PARTITION BY key ORDER BY is_flagged_plus_status_code DESC NULLS LAST, id, key) = 1
),
enriched AS (
    SELECT
        id,
        key,
        is_flagged,
        is_flagged_plus_status_code,
        ROW_NUMBER() OVER w_part AS seq_no,
        LAST_VALUE(key) IGNORE NULLS OVER (PARTITION BY key ORDER BY is_flagged, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_key
    FROM deduped
    WINDOW w_part AS (PARTITION BY key ORDER BY is_flagged, id, key)
),
ranked AS (
    SELECT
        id,
        key,
        is_flagged,
        is_flagged_plus_status_code,
        last_known_key
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY key ORDER BY is_flagged_plus_status_code, id, key) AS latest_rank
        FROM enriched
    ) ranked
    WHERE latest_rank = 1
)
SELECT
    CASE WHEN is_flagged_plus_status_code >= 68 THEN 'LARGE' ELSE 'SMALL' END AS is_flagged_plus_status_code_tier,
    COUNT(*) AS cnt,
    SORT_ARRAY(COLLECT_SET(key)) AS key_list,
    APPROX_PERCENTILE(is_flagged_plus_status_code, 0.95) AS is_flagged_plus_status_code_p95,
    AVG(is_flagged) AS avg_is_flagged,
    MAX(is_flagged) FILTER (WHERE key = 'region') AS region_max_is_flagged,
    COUNT(DISTINCT key) AS distinct_keys
FROM ranked
GROUP BY CASE WHEN is_flagged_plus_status_code >= 68 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY cnt;
