WITH base AS (
    -- Basic cleansing and defaults
    SELECT *, COALESCE(carrier, 'N/A') AS carrier_filled
    FROM shipments
    WHERE delivered_date >= DATE '2023-06-09'
      AND carrier IS NOT NULL
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        MAX(cost) OVER w_part AS last3_max_cost,
        MIN(cost) OVER w_part AS last3_min_cost,
        SUM(cost) OVER w_part AS last3_sum_cost,
        RANK() OVER (PARTITION BY status ORDER BY cost NULLS FIRST) AS cost_rank
    FROM base
    WINDOW w_part AS (PARTITION BY carrier ORDER BY cost, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        order_id,
        shipped_date,
        delivered_date,
        cost,
        carrier_filled,
        last3_max_cost
    FROM scored
    QUALIFY ROW_NUMBER() OVER (PARTITION BY carrier, order_id ORDER BY last3_min_cost DESC NULLS LAST, id) = 1
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        shipped_date,
        cost,
        carrier_filled,
        AVG(cost) OVER w_grp AS last4_avg_cost,
        SUM(cost) OVER w_grp AS last4_sum_cost,
        COUNT(cost) OVER w_grp AS last4_count_cost,
        MAX(cost) OVER w_grp AS last4_max_cost
    FROM deduped
    WHERE order_id IN (18, 36, 40)
    WINDOW w_grp AS (PARTITION BY order_id ORDER BY delivered_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
)
-- Grouped metrics
SELECT
    order_id,
    COUNT(DISTINCT id) AS distinct_ids,
    AVG(last4_count_cost) AS avg_last4_count_cost,
    COUNT(*) AS cnt,
    PERCENTILE_APPROX(last4_max_cost, 0.9) AS last4_max_cost_p90,
    COUNT(DISTINCT id) AS distinct_ids_2
FROM enriched
GROUP BY order_id
ORDER BY order_id;
