WITH ranked AS (
    SELECT *
    FROM order_items
    QUALIFY RANK() OVER (PARTITION BY product_id, order_id ORDER BY unit_price) = 1
),
metrics AS (
    SELECT
        *,
        AVG(unit_price) OVER (ORDER BY unit_price, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS unit_price_avg_3_prior,
        ROUND(quantity / SUM(quantity) OVER (PARTITION BY product_id), 4) AS quantity_share,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY quantity, id) AS event_seq,
        MIN(quantity) OVER (PARTITION BY order_id) AS order_id_min_quantity
    FROM ranked
),
with_windows AS (
    SELECT
        id,
        product_id,
        unit_price_avg_3_prior,
        quantity_share,
        event_seq,
        order_id_min_quantity,
        MIN(quantity_share) OVER (PARTITION BY product_id ORDER BY order_id_min_quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_quantity_share,
        AVG(quantity_share) OVER (PARTITION BY product_id ORDER BY order_id_min_quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_quantity_share,
        COUNT(quantity_share) OVER (PARTITION BY product_id ORDER BY order_id_min_quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_quantity_share,
        CUME_DIST() OVER (PARTITION BY product_id ORDER BY unit_price_avg_3_prior) AS unit_price_avg_3_prior_cume_dist
    FROM metrics
),
deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity_share NULLS FIRST, id) AS latest_rank
    FROM with_windows
    QUALIFY latest_rank = 1
),
with_dims AS (
    SELECT s.*, p.cost
    FROM deduped s
    LEFT JOIN products p
        ON s.product_id = p.id
)
SELECT
    product_id,
    PERCENTILE_APPROX(running_min_quantity_share, 0.9) AS running_min_quantity_share_p90,
    APPROX_PERCENTILE(cost, ARRAY(0.25, 0.5, 0.75)) AS cost_quartiles,
    COUNT(*) AS event_count
FROM with_dims
GROUP BY 1
HAVING PERCENTILE_APPROX(running_min_quantity_share, 0.9) > 0
ORDER BY running_min_quantity_share_p90 NULLS LAST;
