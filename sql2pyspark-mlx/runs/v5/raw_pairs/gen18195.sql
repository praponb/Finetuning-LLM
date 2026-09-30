WITH with_dims AS (
    -- Attach orders attributes
    SELECT
        e.id,
        e.order_id,
        e.product_id,
        e.quantity,
        d.channel,
        d.discount,
        d.status
    FROM order_items e
    LEFT JOIN orders d
        ON e.order_id = d.id
    WHERE d.shipping_fee <= 35
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN discount >= 0.39 THEN 'High' WHEN discount >= 0.16 THEN 'Medium' ELSE 'Low' END AS discount_tier
    FROM with_dims
    WHERE product_id IS NOT NULL
      AND status NOT IN ('CANCELLED', 'COMPLETED', 'PENDING')
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        order_id,
        quantity,
        discount_tier,
        AVG(quantity) OVER (PARTITION BY status ORDER BY discount ASC, id ASC ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS quantity_avg_6_prior,
        LEAD(quantity, 1, 0) OVER (PARTITION BY product_id ORDER BY quantity ASC, id ASC) AS next_quantity
    FROM base
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(quantity_avg_6_prior) OVER (PARTITION BY order_id ORDER BY quantity_avg_6_prior ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_quantity_avg_6_prior,
        AVG(next_quantity) OVER (PARTITION BY order_id ORDER BY quantity_avg_6_prior ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS next_quantity_avg_3,
        AVG(quantity_avg_6_prior) OVER (ORDER BY quantity ASC, id ASC ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS quantity_avg_6_prior_avg_6_prior
    FROM scored
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        order_id,
        cumulative_quantity_avg_6_prior,
        next_quantity_avg_3,
        quantity_avg_6_prior_avg_6_prior
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY next_quantity DESC NULLS FIRST, id ASC) AS latest_rank
        FROM enriched
    ) t
    WHERE latest_rank = 1
)
-- Aggregate with conditional metrics
SELECT
    order_id,
    MAX(cumulative_quantity_avg_6_prior) AS max_cumulative_quantity_avg_6_prior,
    ROUND(AVG(cumulative_quantity_avg_6_prior), 2) AS avg_cumulative_quantity_avg_6_prior,
    MAX(cumulative_quantity_avg_6_prior) AS max_cumulative_quantity_avg_6_prior_2,
    APPROX_PERCENTILE(next_quantity_avg_3, ARRAY(0.25, 0.5, 0.75)) AS next_quantity_avg_3_quartiles,
    COUNT(*) AS cnt
FROM ranked
GROUP BY order_id;
