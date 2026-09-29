WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        DATE_TRUNC('year', shipped_date) shipped_date_period,
        ROUND(cost * 1.15, 2) adjusted_cost,
        COALESCE(carrier, 'NONE') resolved_carrier
    FROM shipments
    WHERE carrier IS DISTINCT FROM 'Kerry'
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM valid
    QUALIFY RANK() OVER (PARTITION BY order_id ORDER BY delivered_date) = 1
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, RANK() OVER (PARTITION BY order_id ORDER BY shipped_date DESC) AS rn
    FROM ranked
    QUALIFY rn = 1
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY carrier ORDER BY adjusted_cost DESC NULLS FIRST) adjusted_cost_percent_rank,
        LAST_VALUE(adjusted_cost, TRUE) OVER (PARTITION BY carrier ORDER BY delivered_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_adjusted_cost,
        SUM(cost) OVER (PARTITION BY carrier) carrier_sum_cost
    FROM deduped
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(last_known_adjusted_cost) OVER (PARTITION BY carrier ORDER BY shipped_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) last7_min_last_known_adjusted_cost,
        COUNT(last_known_adjusted_cost) OVER (PARTITION BY carrier ORDER BY shipped_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) last7_count_last_known_adjusted_cost,
        MAX(last_known_adjusted_cost) OVER (PARTITION BY carrier ORDER BY shipped_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) last7_max_last_known_adjusted_cost,
        LAG(cost, 1) OVER (PARTITION BY order_id ORDER BY cost, id) prev_cost
    FROM metrics
    WHERE cost < 58
),
metrics_shipments AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY last7_max_last_known_adjusted_cost, id) seq_no,
        FIRST_VALUE(adjusted_cost, TRUE) OVER (PARTITION BY order_id ORDER BY adjusted_cost_percent_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_adjusted_cost,
        LEAD(cost, 2, 0) OVER w_running next_cost,
        MAX(carrier_sum_cost) OVER w_recent carrier_sum_cost_max_90d,
        LAG(prev_cost, 2) OVER w_ordered prev_prev_cost
    FROM enriched
    WINDOW w_running AS (PARTITION BY carrier ORDER BY delivered_date, id), w_recent AS (ORDER BY CAST(shipped_date_period AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW), w_ordered AS (PARTITION BY order_id ORDER BY carrier_sum_cost, id)
),
wide AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            status,
            carrier,
            last7_count_last_known_adjusted_cost
        FROM metrics_shipments
    ) src
    PIVOT (
        SUM(last7_count_last_known_adjusted_cost) AS value_sum,
        COUNT(1) AS n
        FOR carrier IN ('DHL' AS dhl, 'Flash' AS flash, 'UPS' AS ups)
    )
)
SELECT
    status,
    dhl_value_sum,
    dhl_n,
    flash_value_sum,
    flash_n,
    ups_value_sum,
    ups_n
FROM wide
ORDER BY dhl_n NULLS FIRST;
