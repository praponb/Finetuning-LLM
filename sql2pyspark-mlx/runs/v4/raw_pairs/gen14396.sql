SELECT status, YEAR(order_date) AS order_year, SORT_ARRAY(COLLECT_SET(channel)) AS channel_list, COUNT(amount) AS amount_count FROM orders GROUP BY status, order_year
