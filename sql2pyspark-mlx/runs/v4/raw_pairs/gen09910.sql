SELECT
    order_date,
    customer_id,
    CAST(shipping_fee AS INT) AS shipping_fee_int,
    TRIM(status) AS clean_status
FROM orders
WHERE customer_id = 14;
