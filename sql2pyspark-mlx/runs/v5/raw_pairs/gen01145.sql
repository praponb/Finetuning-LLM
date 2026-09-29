WITH scored AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(cost) OVER (PARTITION BY carrier ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_cost,
        ROW_NUMBER() OVER w_ordered AS event_seq
    FROM shipments
    WHERE shipped_date <= '2023-06-01'
    WINDOW w_ordered AS (PARTITION BY carrier, status ORDER BY cost DESC, id)
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        carrier,
        delivered_date,
        cost,
        status,
        running_cost,
        event_seq,
        CONCAT_WS('-', status, carrier) AS status_carrier_key,
        COALESCE(carrier, 'UNKNOWN') AS resolved_carrier
    FROM scored
    WHERE event_seq > 6
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        carrier,
        event_seq,
        status_carrier_key,
        SUM(cost) OVER (ORDER BY running_cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_cost,
        LEAD(cost, 1) OVER (PARTITION BY carrier, status ORDER BY event_seq, id) AS next_cost
    FROM filtered
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(next_cost) OVER (ORDER BY event_seq, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS next_cost_avg_5,
        FIRST_VALUE(cumulative_cost) IGNORE NULLS OVER (PARTITION BY carrier ORDER BY event_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_cumulative_cost,
        COUNT(next_cost) OVER (PARTITION BY carrier) AS carrier_count_next_cost,
        AVG(next_cost) OVER (PARTITION BY carrier ORDER BY next_cost, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS next_cost_avg_5_2
    FROM with_windows
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        carrier,
        event_seq,
        status_carrier_key,
        cumulative_cost,
        next_cost_avg_5,
        carrier_count_next_cost,
        STDDEV_SAMP(carrier_count_next_cost) OVER (PARTITION BY carrier) AS carrier_stddev_samp_carrier_count_next_cost,
        ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY next_cost, id) AS row_seq,
        CUME_DIST() OVER (ORDER BY next_cost NULLS LAST) AS next_cost_cume_dist
    FROM metrics
)
-- Aggregate with conditional metrics
SELECT
    carrier,
    CASE WHEN carrier_count_next_cost >= 13 THEN 'PREMIUM' ELSE 'STANDARD' END AS carrier_count_next_cost_tier,
    SUM(next_cost_avg_5) AS total_next_cost_avg_5,
    COUNT(DISTINCT carrier) AS distinct_carriers
FROM enriched
GROUP BY GROUPING SETS ((carrier, CASE WHEN carrier_count_next_cost >= 13 THEN 'PREMIUM' ELSE 'STANDARD' END), (carrier), ())
ORDER BY carrier
