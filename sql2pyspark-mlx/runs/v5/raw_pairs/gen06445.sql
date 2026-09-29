WITH ranked AS (
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock
    FROM inventory
    QUALIFY ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY last_restock DESC NULLS LAST, id ASC) = 1
),
filtered AS (
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        DATE_TRUNC('year', last_restock) last_restock_period
    FROM ranked
    WHERE last_restock >= TO_DATE('2023-03-23')
),
with_gaps AS (
    SELECT *, CASE WHEN LAG(last_restock_period) OVER (PARTITION BY product_id ORDER BY last_restock_period ASC, id ASC) IS NULL
                 OR last_restock_period > LAG(last_restock_period) OVER (PARTITION BY product_id ORDER BY last_restock_period ASC, id ASC) + INTERVAL '90' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM filtered
),
islands AS (
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        last_restock_period,
        SUM(is_new_session) OVER (PARTITION BY product_id ORDER BY last_restock_period ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
),
prev_events AS (
    SELECT *, LAG(last_restock_period) OVER (PARTITION BY product_id ORDER BY last_restock_period ASC, id ASC) AS prev_last_restock_period
    FROM islands
),
gaps AS (
    SELECT *, CASE WHEN prev_last_restock_period IS NULL OR last_restock_period > prev_last_restock_period + INTERVAL '14' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM prev_events
),
islands_inventory AS (
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock_period,
        SUM(is_new_session) OVER (PARTITION BY product_id ORDER BY last_restock_period ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
),
deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY quantity ASC NULLS FIRST, id ASC) AS rank_in_group
    FROM islands_inventory
    QUALIFY rank_in_group = 1
)
SELECT
    s.id,
    s.product_id,
    s.quantity,
    (SELECT MAX(o2.unit_price) FROM order_items o2 WHERE o2.product_id = s.product_id) AS max_order_item_unit_price
FROM deduped s
