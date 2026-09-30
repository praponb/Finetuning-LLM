WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        CASE WHEN quantity >= 183 THEN 'High' WHEN quantity >= 32 THEN 'Medium' ELSE 'Low' END AS quantity_band,
        DATEDIFF(CURRENT_DATE(), last_restock) AS days_since_last_restock
    FROM inventory
    WHERE last_restock > TO_DATE('2023-09-28')
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        product_id,
        quantity,
        quantity_band,
        LEAD(quantity, 1, 0) OVER (PARTITION BY store_id ORDER BY days_since_last_restock, id) AS next_quantity,
        STDDEV_SAMP(days_since_last_restock) OVER (PARTITION BY store_id) AS store_id_stddev_samp_days_since_last_restock,
        NTILE(3) OVER (PARTITION BY store_id, product_id ORDER BY quantity DESC, id) AS quantity_bucket
    FROM valid
),
with_dims AS (
    -- Attach products attributes
    SELECT
        a.*,
        p.category,
        p.price
    FROM scored a
    LEFT JOIN products p
        ON a.product_id = p.id
),
scored_inventory AS (
    -- Running and moving aggregates
    SELECT
        id,
        product_id,
        category,
        SUM(price) OVER (PARTITION BY product_id, category ORDER BY price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_price,
        LAST_VALUE(quantity) IGNORE NULLS OVER (PARTITION BY product_id ORDER BY store_id_stddev_samp_days_since_last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_quantity,
        AVG(price) OVER (PARTITION BY product_id ORDER BY quantity_bucket, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS price_avg_5_prior
    FROM with_dims
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, cumulative_price + last_known_quantity AS cumulative_price_plus_last_known_quantity
    FROM scored_inventory
    WHERE category IS DISTINCT FROM 'Electronics'
      AND last_known_quantity = 192
),
joined AS (
    -- Attach products attributes
    SELECT s.*, p.category AS product_category
    FROM filtered s
    LEFT JOIN products p
        ON s.product_id = p.id
),
scored_inventory_2 AS (
    -- Running and moving aggregates
    SELECT
        id,
        category,
        cumulative_price,
        last_known_quantity,
        LAG(cumulative_price_plus_last_known_quantity, 1) OVER w_grp AS prev_cumulative_price_plus_last_known_quantity,
        AVG(last_known_quantity) OVER (PARTITION BY product_category ORDER BY last_known_quantity, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS last_known_quantity_avg_3_prior,
        LEAD(price_avg_5_prior, 1) OVER (PARTITION BY product_id ORDER BY cumulative_price, id) AS next_price_avg_5_prior
    FROM joined
    WHERE last_known_quantity = 218
    WINDOW w_grp AS (PARTITION BY category, product_category ORDER BY price_avg_5_prior, id)
)
-- Grouped metrics
SELECT
    category,
    CASE WHEN prev_cumulative_price_plus_last_known_quantity >= 76 THEN 'LARGE' ELSE 'SMALL' END AS prev_cumulative_price_plus_last_known_quantity_tier,
    MIN(prev_cumulative_price_plus_last_known_quantity) AS min_prev_cumulative_price_plus_last_known_quantity,
    SORT_ARRAY(COLLECT_SET(category)) AS category_list,
    SORT_ARRAY(COLLECT_SET(category)) AS category_list_2
FROM scored_inventory_2
GROUP BY 1, 2
ORDER BY category
