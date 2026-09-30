WITH exploded AS (
    SELECT
        id,
        service,
        attr_key,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS attr_key, value
),
cleaned AS (
    SELECT *, COALESCE(attr_key, 'Unknown') AS attr_key_filled
    FROM exploded
    WHERE attr_key IS NOT NULL
),
base AS (
    SELECT *, CASE WHEN attr_key = 'region' THEN 1 ELSE 0 END AS is_region
    FROM cleaned
    WHERE attr_key IS NOT NULL
      AND attr_key IN ('version', 'region')
),
scored AS (
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY service ORDER BY is_region DESC NULLS LAST) AS is_region_dense_rank,
        MAX(is_region) OVER (PARTITION BY attr_key) AS attr_key_max_is_region,
        RANK() OVER (PARTITION BY attr_key ORDER BY is_region NULLS FIRST) AS is_region_rank,
        MAX(is_region) OVER (PARTITION BY attr_key) AS attr_key_max_is_region_2
    FROM base
)
SELECT
    service,
    CASE WHEN is_region_dense_rank >= 7 THEN 'LARGE' ELSE 'SMALL' END AS is_region_dense_rank_tier,
    attr_key,
    COUNT(DISTINCT id) AS distinct_ids,
    MAX(is_region_dense_rank) AS max_is_region_dense_rank,
    COUNT(*) FILTER (WHERE attr_key = 'trace') AS trace_rows,
    COUNT_IF(attr_key = 'region') AS region_events
FROM scored
GROUP BY service, CASE WHEN is_region_dense_rank >= 7 THEN 'LARGE' ELSE 'SMALL' END, attr_key
ORDER BY max_is_region_dense_rank DESC NULLS FIRST;
