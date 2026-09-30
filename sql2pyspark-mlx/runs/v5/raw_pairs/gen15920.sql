WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, UPPER(carrier) AS carrier_upper
    FROM shipments
    WHERE order_id IS NOT NULL
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        COUNT(cost) OVER (PARTITION BY status, carrier ORDER BY delivered_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_cost,
        MIN(cost) OVER (PARTITION BY status, carrier ORDER BY delivered_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_cost,
        SUM(cost) OVER (PARTITION BY status, carrier ORDER BY delivered_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_cost,
        MAX(cost) OVER (PARTITION BY status, carrier ORDER BY delivered_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_cost,
        LAG(cost, 1, 0) OVER (PARTITION BY carrier ORDER BY shipped_date, id) AS prev_cost
    FROM cleaned
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY running_count_cost NULLS LAST, id) AS rank_in_group
        FROM enriched
    ) x
    WHERE rank_in_group = 1
),
joined AS (
    -- Attach orders attributes
    SELECT
        s.id,
        s.order_id,
        s.shipped_date,
        s.delivered_date,
        s.carrier_upper,
        s.running_count_cost,
        s.running_min_cost,
        s.prev_cost,
        s.rank_in_group,
        b.discount
    FROM deduped s
    LEFT JOIN orders b
        ON s.order_id <=> b.id
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        carrier_upper,
        running_count_cost,
        rank_in_group,
        GREATEST(running_min_cost, discount) AS greatest_running_min_cost_discount
    FROM joined
    WHERE shipped_date > DATE '2023-02-21'
      AND discount BETWEEN 0.32 AND 0.37
),
enriched_shipments AS (
    -- Ranking and gap features
    SELECT
        id,
        greatest_running_min_cost_discount,
        LAG(running_count_cost) OVER (PARTITION BY order_id ORDER BY greatest_running_min_cost_discount, id) AS prev_running_count_cost,
        LAG(greatest_running_min_cost_discount) OVER (PARTITION BY order_id ORDER BY greatest_running_min_cost_discount, id) AS prev_greatest_running_min_cost_discount,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY greatest_running_min_cost_discount, id) AS event_seq,
        AVG(running_count_cost) OVER (PARTITION BY order_id ORDER BY rank_in_group, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS running_count_cost_avg_3_prior
    FROM base
    WHERE order_id = 23
)
-- Grouped metrics
SELECT
    CASE WHEN greatest_running_min_cost_discount >= 78 THEN 'LARGE' ELSE 'SMALL' END AS greatest_running_min_cost_discount_tier,
    COUNT(DISTINCT id) AS distinct_ids,
    PERCENTILE_APPROX(event_seq, 0.5) AS event_seq_p50,
    MIN(greatest_running_min_cost_discount) AS min_greatest_running_min_cost_discount
FROM enriched_shipments
GROUP BY greatest_running_min_cost_discount_tier;
