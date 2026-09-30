SELECT
    id,
    product_id,
    unit_price,
    PERCENT_RANK() OVER (PARTITION BY product_id ORDER BY unit_price DESC) AS unit_price_pct_rank
FROM order_items
WHERE order_id = 20
