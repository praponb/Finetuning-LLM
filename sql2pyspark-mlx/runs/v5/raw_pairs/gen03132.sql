WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CEIL(cost) cost_whole,
        INITCAP(TRIM(status)) status_title,
        COALESCE(carrier, 'N/A') resolved_carrier
    FROM shipments
    WHERE shipped_date > DATE '2023-03-17'
),
matched_keys AS (
    -- Set difference on keys
    SELECT status
    FROM filtered
    EXCEPT ALL
    SELECT status
    FROM shipments
    WHERE delivered_date > '2025-08-01'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    JOIN matched_keys k ON s.status = k.status
),
joined AS (
    -- Attach orders attributes
    SELECT a.*, b.channel
    FROM kept_rows a
    JOIN orders b
        ON a.order_id <=> b.id
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        cost_whole,
        status_title,
        resolved_carrier,
        DATE_ADD(delivered_date, 25) delivered_date_plus_25d
    FROM joined
    WHERE delivered_date >= DATE '2023-06-28'
      AND order_id IS NOT NULL
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        carrier,
        delivered_date,
        cost_whole,
        PERCENT_RANK() OVER (ORDER BY cost_whole DESC NULLS LAST) cost_whole_percent_rank,
        RANK() OVER (ORDER BY cost_whole DESC NULLS FIRST) cost_whole_rank,
        STDDEV_SAMP(cost_whole) OVER (PARTITION BY carrier) carrier_stddev_samp_cost_whole
    FROM cleaned
    WHERE cost < 90
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        carrier,
        cost_whole_percent_rank,
        PERCENT_RANK() OVER (ORDER BY cost_whole_rank DESC NULLS LAST) cost_whole_rank_percent_rank,
        AVG(cost_whole) OVER (PARTITION BY carrier) carrier_avg_cost_whole
    FROM metrics
),
enriched_shipments AS (
    -- Ranking and gap features
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY carrier ORDER BY cost_whole_rank_percent_rank DESC NULLS FIRST) cost_whole_rank_percent_rank_percent_rank,
        RANK() OVER (PARTITION BY carrier ORDER BY cost_whole_percent_rank NULLS FIRST) cost_whole_percent_rank_rank,
        DENSE_RANK() OVER (PARTITION BY carrier ORDER BY carrier_avg_cost_whole) carrier_avg_cost_whole_dense_rank
    FROM enriched
),
totals AS (
    -- Multi-level aggregation
    SELECT
        carrier,
        SUM(cost_whole_rank_percent_rank_percent_rank) FILTER (WHERE carrier = 'UPS') AS ups_total_cost_whole_rank_percent_rank_percent_rank,
        APPROX_PERCENTILE(carrier_avg_cost_whole, ARRAY(0.25, 0.5, 0.75)) AS carrier_avg_cost_whole_quartiles,
        GROUPING(carrier) AS grouping_id
    FROM enriched_shipments
    GROUP BY CUBE (carrier)
    HAVING ups_total_cost_whole_rank_percent_rank_percent_rank > 0
)
SELECT carrier_avg_cost_whole_quartiles, grouping_id
FROM totals;
