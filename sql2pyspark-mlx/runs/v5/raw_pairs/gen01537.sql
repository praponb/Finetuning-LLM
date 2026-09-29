WITH exploded AS (
    SELECT
        id,
        log_ts,
        level,
        latency_ms,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS attr_key, attr_value
),
ranked AS (
    SELECT
        id,
        level,
        latency_ms,
        attr_key,
        attr_value
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY level, attr_key ORDER BY log_ts DESC, id, attr_key) AS rank_in_group
        FROM exploded
    ) ranked
    WHERE rank_in_group = 1
),
scored AS (
    SELECT
        id,
        level,
        attr_key,
        MIN(latency_ms) OVER (PARTITION BY attr_key) attr_key_min_latency_ms,
        FIRST_VALUE(latency_ms, TRUE) OVER (PARTITION BY level ORDER BY latency_ms, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_latency_ms
    FROM ranked
    WHERE attr_key NOT IN ('region', 'trace')
),
ranked_app_logs AS (
    SELECT id, attr_key
    FROM scored
    QUALIFY ROW_NUMBER() OVER (PARTITION BY attr_key ORDER BY first_known_latency_ms NULLS FIRST, id, attr_key) = 1
)
SELECT
    attr_key,
    COUNT(*) AS cnt,
    COUNT_IF(attr_key = 'version') AS version_events,
    SIZE(COLLECT_SET(attr_key)) AS num_attr_keys,
    SIZE(COLLECT_SET(attr_key)) AS num_attr_keys_2
FROM ranked_app_logs
GROUP BY ROLLUP (attr_key)
ORDER BY cnt NULLS FIRST;
