SELECT
    id,
    order_id,
    product_id,
    quantity,
    unit_price,
    PERCENT_RANK() OVER w_running quantity_percent_rank,
    CORR(unit_price, quantity) OVER (PARTITION BY order_id) corr_unit_price_quantity,
    SUM(quantity) OVER w_running_2 running_quantity,
    RANK() OVER (PARTITION BY order_id ORDER BY quantity NULLS FIRST) quantity_rank,
    SUM(unit_price) OVER (PARTITION BY product_id) product_id_sum_unit_price
FROM order_items
WINDOW w_running AS (ORDER BY quantity DESC), w_running_2 AS (ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);
