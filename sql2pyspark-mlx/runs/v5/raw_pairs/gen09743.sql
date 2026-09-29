WITH numbered AS (
    SELECT
        id,
        customer_id,
        order_date,
        shipping_fee,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date DESC, id) AS row_num
    FROM orders
    WHERE discount >= 0.36
)
SELECT *
FROM numbered
WHERE row_num <= 3;
