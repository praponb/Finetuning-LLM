WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        delivered_date,
        status,
        CONCAT_WS(',', status, carrier) status_carrier_key,
        COALESCE(carrier, 'OTHER') resolved_carrier
    FROM shipments
    WHERE carrier IS NOT NULL
      AND shipped_date < '2023-06-01'
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        status,
        status_carrier_key,
        DATE_TRUNC('WEEK', delivered_date) delivered_date_period,
        LENGTH(status) status_length
    FROM cleaned
    WHERE YEAR(delivered_date) = 2024
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, RANK() OVER (PARTITION BY status ORDER BY status_length DESC NULLS FIRST) AS rank_in_group
    FROM filtered
    QUALIFY rank_in_group = 1
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(delivered_date_period) OVER (PARTITION BY status ORDER BY delivered_date_period, id) IS NULL
                 OR delivered_date_period > LAG(delivered_date_period) OVER (PARTITION BY status ORDER BY delivered_date_period, id) + INTERVAL '60' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM latest
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        status,
        status_carrier_key,
        delivered_date_period,
        rank_in_group,
        SUM(is_new_burst) OVER (PARTITION BY status ORDER BY delivered_date_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
flagged_2 AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(delivered_date_period) OVER (PARTITION BY status ORDER BY delivered_date_period, id) IS NULL
                 OR delivered_date_period > LAG(delivered_date_period) OVER (PARTITION BY status ORDER BY delivered_date_period, id) + INTERVAL '7' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM islands
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY status ORDER BY delivered_date_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged_2
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        status,
        rank_in_group,
        burst_id
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY rank_in_group, id) AS rank_in_group_2
        FROM sessionized
    ) x
    WHERE rank_in_group_2 = 1
)
-- Multi-level aggregation
SELECT
    CASE WHEN burst_id >= 6 THEN 'PREMIUM' ELSE 'STANDARD' END AS burst_id_tier,
    COUNT(CASE WHEN status = 'RETURNED' THEN 1 END) AS returned_count,
    MIN(burst_id) AS min_burst_id,
    COUNT(CASE WHEN status = 'IN_TRANSIT' THEN 1 END) AS in_transit_count,
    COUNT(DISTINCT status) AS distinct_status,
    ROUND(SUM(burst_id) / NULLIF(COUNT(CASE WHEN status = 'DELIVERED' THEN 1 END), 0), 2) AS burst_id_per_delivered
FROM deduped
GROUP BY CASE WHEN burst_id >= 6 THEN 'PREMIUM' ELSE 'STANDARD' END
HAVING COUNT(CASE WHEN status = 'RETURNED' THEN 1 END) > 1
