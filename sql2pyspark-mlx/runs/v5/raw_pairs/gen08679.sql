SELECT
    QUARTER(stores.opened_date) AS opened_quarter,
    stores.region,
    ROUND(AVG(orders.discount), 2) AS avg_discount,
    MAX(orders.order_date) AS last_order_date
FROM orders
INNER JOIN stores ON orders.store_id = stores.id
INNER JOIN customers ON orders.customer_id = customers.id
WHERE customers.country <> 'UK'
GROUP BY QUARTER(stores.opened_date), stores.region
ORDER BY opened_quarter DESC;
