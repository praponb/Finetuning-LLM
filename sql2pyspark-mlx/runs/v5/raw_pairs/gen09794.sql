WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        product_id,
        COALESCE(quantity, 0) AS quantity_filled,
        CASE WHEN quantity >= 219 THEN 'High' WHEN quantity >= 116 THEN 'Medium' ELSE 'Low' END AS quantity_tier
    FROM inventory
    WHERE quantity BETWEEN 108 AND 207
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(quantity_filled) OVER (PARTITION BY product_id ORDER BY quantity_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_quantity_filled,
        CUME_DIST() OVER (ORDER BY quantity_filled NULLS FIRST) AS quantity_filled_cume_dist,
        SUM(quantity_filled) OVER (ORDER BY quantity_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_quantity_filled_2,
        AVG(quantity_filled) OVER (PARTITION BY store_id ORDER BY quantity_filled, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS quantity_filled_avg_3_prior,
        SUM(quantity_filled) OVER (ORDER BY quantity_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_quantity_filled_3
    FROM valid
    WHERE product_id IN (2, 4, 9)
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(quantity_filled_cume_dist) OVER (PARTITION BY product_id, store_id ORDER BY quantity_filled, id) AS prev_quantity_filled_cume_dist,
        LEAD(quantity_filled_cume_dist) OVER (PARTITION BY product_id, store_id ORDER BY quantity_filled, id) AS next_quantity_filled_cume_dist,
        LAG(cumulative_quantity_filled) OVER (PARTITION BY product_id, store_id ORDER BY quantity_filled, id) AS prev_cumulative_quantity_filled,
        LEAD(cumulative_quantity_filled) OVER (PARTITION BY product_id, store_id ORDER BY quantity_filled, id) AS next_cumulative_quantity_filled,
        ROW_NUMBER() OVER (PARTITION BY product_id, store_id ORDER BY quantity_filled, id) AS event_seq
    FROM metrics
),
matched_keys AS (
    -- Set difference on keys
    SELECT id, product_id
    FROM scored
    EXCEPT DISTINCT
    SELECT id, product_id
    FROM inventory
    WHERE last_restock >= '2023-07-01'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM scored s
    JOIN matched_keys k ON s.id = k.id AND s.product_id = k.product_id
)
SELECT
    s.id,
    s.store_id,
    s.quantity_tier,
    s.cumulative_quantity_filled,
    s.quantity_filled_cume_dist,
    s.cumulative_quantity_filled_2,
    s.quantity_filled_avg_3_prior,
    s.prev_quantity_filled_cume_dist,
    s.next_quantity_filled_cume_dist,
    (SELECT AVG(t2.amount) FROM orders t2 WHERE t2.store_id = s.store_id) AS avg_order_amount
FROM verified s
ORDER BY next_quantity_filled_cume_dist DESC NULLS LAST;
