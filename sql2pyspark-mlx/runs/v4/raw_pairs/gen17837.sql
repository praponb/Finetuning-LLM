WITH deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY order_id ORDER BY unit_price NULLS FIRST) AS row_num
        FROM order_items
    ) ranked
    WHERE row_num = 1
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY order_id ORDER BY quantity NULLS LAST) AS latest_rank
        FROM deduped
    ) x
    WHERE latest_rank = 1
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        row_num,
        latest_rank,
        LAG(unit_price, 1) OVER (ORDER BY quantity, id) AS prev_unit_price,
        SUM(quantity) OVER (PARTITION BY order_id, product_id ORDER BY latest_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_quantity,
        RANK() OVER (PARTITION BY order_id ORDER BY quantity NULLS LAST) AS quantity_rank,
        LEAD(quantity, 1, 0) OVER (PARTITION BY order_id, product_id ORDER BY latest_rank, id) AS next_quantity,
        CORR(unit_price, quantity) OVER (PARTITION BY product_id) AS corr_unit_price_quantity
    FROM ranked
),
ranked_order_items AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY corr_unit_price_quantity DESC, id) AS rank_in_group
    FROM with_windows
    QUALIFY rank_in_group = 1
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN quantity_rank >= 8 THEN 'HIGH' ELSE 'LOW' END AS quantity_rank_tier,
        COUNT(*) AS event_count,
        ROUND(AVG(cumulative_quantity), 2) AS avg_cumulative_quantity,
        BIT_AND(quantity) AS quantity_bit_and
    FROM ranked_order_items
    GROUP BY CASE WHEN quantity_rank >= 8 THEN 'HIGH' ELSE 'LOW' END
    HAVING event_count < 1
)
SELECT quantity_rank_tier, avg_cumulative_quantity
FROM summary
ORDER BY quantity_rank_tier;
