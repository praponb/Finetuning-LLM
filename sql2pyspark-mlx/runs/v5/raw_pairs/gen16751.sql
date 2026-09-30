WITH enriched AS (
    SELECT
        s.*,
        p.region,
        p.store_name
    FROM inventory s
    INNER JOIN stores p
        ON s.store_id = p.id
),
scored AS (
    SELECT
        id,
        product_id,
        last_restock,
        region,
        SUM(quantity) OVER (PARTITION BY region ORDER BY last_restock, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_sum_quantity,
        MAX(quantity) OVER (PARTITION BY region ORDER BY last_restock, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_max_quantity,
        MIN(quantity) OVER (PARTITION BY region ORDER BY last_restock, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_min_quantity
    FROM enriched
    WHERE store_id = 2
),
enriched_inventory AS (
    SELECT
        *,
        NTILE(4) OVER (ORDER BY last4_max_quantity DESC, id) AS last4_max_quantity_bucket,
        RANK() OVER (PARTITION BY product_id ORDER BY last4_max_quantity DESC NULLS FIRST) AS last4_max_quantity_rank,
        ROW_NUMBER() OVER w_ordered AS row_seq,
        SUM(last4_sum_quantity) OVER w_running AS product_id_sum_last4_sum_quantity,
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY last4_sum_quantity DESC, id) AS event_seq
    FROM scored
    WINDOW w_ordered AS (PARTITION BY product_id ORDER BY last_restock DESC, id), w_running AS (PARTITION BY product_id)
),
joined AS (
    SELECT
        s.id,
        s.product_id,
        s.last_restock,
        s.last4_sum_quantity,
        s.last4_max_quantity,
        s.last4_min_quantity,
        s.last4_max_quantity_rank,
        p.cost,
        p.stock_qty
    FROM enriched_inventory s
    LEFT JOIN products p
        ON s.product_id = p.id
),
valid AS (
    SELECT
        id,
        last4_max_quantity,
        last4_max_quantity_rank,
        last4_max_quantity * stock_qty AS last4_max_quantity_times_stock_qty
    FROM joined
    WHERE last4_max_quantity < 180
)
SELECT
    CASE WHEN last4_max_quantity >= 204 THEN 'PREMIUM' ELSE 'STANDARD' END AS last4_max_quantity_tier,
    CASE WHEN last4_max_quantity_rank >= 7 THEN 'HIGH' ELSE 'LOW' END AS last4_max_quantity_rank_tier,
    AVG(last4_max_quantity_rank) AS avg_last4_max_quantity_rank,
    PERCENTILE_APPROX(last4_max_quantity, ARRAY(0.25, 0.5, 0.75)) AS last4_max_quantity_quartiles
FROM valid
GROUP BY ROLLUP (CASE WHEN last4_max_quantity >= 204 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN last4_max_quantity_rank >= 7 THEN 'HIGH' ELSE 'LOW' END)
ORDER BY last4_max_quantity_tier DESC;
