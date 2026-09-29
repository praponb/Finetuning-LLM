SELECT
    *,
    ROW_NUMBER() OVER (ORDER BY unit_price, id) row_seq,
    LAST_VALUE(quantity) IGNORE NULLS OVER (PARTITION BY product_id, order_id ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_quantity
FROM order_items
