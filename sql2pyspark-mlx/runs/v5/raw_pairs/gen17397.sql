SELECT QUARTER(c.signup_date) AS signup_quarter, SORT_ARRAY(COLLECT_SET(o.channel)) AS channel_list, ROUND(STDDEV(o.amount), 2) AS stddev_amount
FROM orders o
LEFT JOIN customers c ON o.customer_id = c.id
GROUP BY QUARTER(c.signup_date)
