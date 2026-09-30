WITH metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        LAG(quantity, 2) OVER (ORDER BY last_restock, id) AS prev_quantity,
        LEAD(quantity, 2) OVER (PARTITION BY store_id ORDER BY quantity, id) AS next_quantity,
        AVG(quantity) OVER (ORDER BY quantity, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS quantity_avg_2_prior,
        AVG(quantity) OVER (PARTITION BY store_id ORDER BY quantity, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS quantity_avg_2,
        SUM(quantity) OVER w_part AS cumulative_quantity
    FROM inventory
    WINDOW w_part AS (PARTITION BY store_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_dims AS (
    -- Attach products attributes
    SELECT a.*, p.cost
    FROM metrics a
    LEFT JOIN products p
        ON a.product_id = p.id
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(cost, 1, 0) OVER (ORDER BY quantity_avg_2_prior, id) AS prev_cost,
        LAG(next_quantity, 1) OVER (ORDER BY cumulative_quantity, id) AS prev_next_quantity,
        CUME_DIST() OVER w_running AS quantity_avg_2_prior_cume_dist,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY prev_quantity DESC, id) AS row_seq
    FROM with_dims
    WINDOW w_running AS (PARTITION BY store_id ORDER BY quantity_avg_2_prior DESC NULLS LAST)
),
metrics_inventory AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY cost DESC, id) AS row_seq_2,
        AVG(prev_next_quantity) OVER (ORDER BY prev_next_quantity, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS prev_next_quantity_avg_3
    FROM scored
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('QUARTER', last_restock) AS quarter_start,
    DATE_TRUNC('WEEK', last_restock) AS week_start,
    ROUND(AVG(cumulative_quantity), 2) AS avg_cumulative_quantity,
    BIT_AND(quantity) AS quantity_bit_and,
    COUNT(*) AS row_count,
    BIT_OR(quantity) AS quantity_bit_or
FROM metrics_inventory
GROUP BY quarter_start, week_start
