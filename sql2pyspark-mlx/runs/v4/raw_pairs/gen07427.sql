WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        level,
        attr_key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS attr_key, value
)
-- Grouped metrics
SELECT
    attr_key,
    host,
    COUNT_IF(attr_key = 'region') AS region_events,
    COUNT_IF(host = 'h2') AS h2_events,
    SIZE(COLLECT_SET(attr_key)) AS num_attr_keys,
    RANK() OVER (PARTITION BY attr_key ORDER BY COUNT_IF(host = 'h2') DESC) AS h2_events_rank
FROM lines
GROUP BY 1, 2
ORDER BY num_attr_keys NULLS FIRST;
