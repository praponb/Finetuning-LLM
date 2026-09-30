WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        carrier,
        delivered_date,
        cost,
        CASE WHEN cost >= 84 THEN 'High' WHEN cost >= 28 THEN 'Medium' ELSE 'Low' END cost_level,
        CONCAT(carrier, '/', status) carrier_status,
        COALESCE(carrier, 'OTHER') carrier_filled
    FROM shipments
    WHERE delivered_date >= DATE '2023-04-14'
),
valid_shipments AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        carrier,
        delivered_date,
        cost,
        cost_level,
        carrier_status,
        carrier_filled,
        CASE WHEN carrier = 'UPS' THEN 1 ELSE 0 END is_ups,
        ADD_MONTHS(delivered_date, 1) delivered_date_plus_months
    FROM valid
    WHERE delivered_date > TO_DATE('2023-10-01')
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        carrier,
        delivered_date,
        cost_level,
        COUNT(is_ups) OVER w_part carrier_count_is_ups,
        MIN(cost) OVER w_part carrier_min_cost
    FROM valid_shipments
    WHERE MONTH(delivered_date) IN (1, 4, 6)
    WINDOW w_part AS (PARTITION BY carrier)
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(carrier_count_is_ups) OVER w_part prev_carrier_count_is_ups,
        LEAD(carrier_count_is_ups) OVER w_part next_carrier_count_is_ups,
        LEAD(carrier_min_cost) OVER w_part next_carrier_min_cost,
        ROW_NUMBER() OVER w_part event_seq
    FROM with_windows
    WINDOW w_part AS (PARTITION BY carrier ORDER BY delivered_date ASC, id ASC)
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        carrier,
        cost_level,
        carrier_min_cost,
        next_carrier_min_cost,
        event_seq,
        LEAD(next_carrier_count_is_ups) OVER (PARTITION BY carrier ORDER BY next_carrier_min_cost ASC, id ASC) next_next_carrier_count_is_ups,
        LAG(next_carrier_min_cost) OVER (PARTITION BY carrier ORDER BY next_carrier_min_cost ASC, id ASC) prev_next_carrier_min_cost,
        ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY next_carrier_min_cost ASC, id ASC) row_seq
    FROM enriched
),
enriched_shipments AS (
    -- Running and moving aggregates
    SELECT
        id,
        carrier,
        carrier_min_cost,
        prev_next_carrier_min_cost,
        SUM(prev_next_carrier_min_cost) OVER (PARTITION BY carrier) carrier_sum_prev_next_carrier_min_cost,
        SUM(next_next_carrier_count_is_ups) OVER (ORDER BY event_seq ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_next_next_carrier_count_is_ups
    FROM scored
)
-- Multi-level aggregation
SELECT
    CASE WHEN prev_next_carrier_min_cost >= 84 THEN 'PREMIUM' ELSE 'STANDARD' END AS prev_next_carrier_min_cost_tier,
    carrier,
    PERCENTILE_APPROX(prev_next_carrier_min_cost, 0.5) AS prev_next_carrier_min_cost_p50,
    SIZE(COLLECT_SET(carrier)) AS num_carriers,
    COUNT_IF(carrier = 'Kerry') AS kerry_events,
    AVG(carrier_sum_prev_next_carrier_min_cost) FILTER (WHERE carrier = 'Flash') AS flash_avg_carrier_sum_prev_next_carrier_min_cost,
    COUNT(*) AS cnt
FROM enriched_shipments
GROUP BY GROUPING SETS ((CASE WHEN prev_next_carrier_min_cost >= 84 THEN 'PREMIUM' ELSE 'STANDARD' END, carrier), (CASE WHEN prev_next_carrier_min_cost >= 84 THEN 'PREMIUM' ELSE 'STANDARD' END))
ORDER BY prev_next_carrier_min_cost_p50 ASC;
