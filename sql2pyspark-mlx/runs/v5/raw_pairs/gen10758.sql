SELECT
    src.id,
    src.customer_id,
    src.store_id,
    src.order_date,
    src.amount,
    src.discount,
    b.sqft,
    b.city
FROM orders src
INNER JOIN stores b
    ON src.store_id <=> b.id
WHERE b.opened_date BETWEEN '2024-02-14' AND '2024-10-11'
