SELECT QUARTER(customers.signup_date) AS signup_quarter, SUM(CASE WHEN orders.status = 'SHIPPED' THEN orders.amount ELSE 0 END) AS shipped_amount, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(orders.channel))) AS channel_values
FROM orders
FULL OUTER JOIN stores ON stores.id = orders.store_id
INNER JOIN customers ON customers.id = orders.customer_id
WHERE orders.discount <= 0.29
GROUP BY QUARTER(customers.signup_date)
ORDER BY 1 DESC, shipped_amount DESC
