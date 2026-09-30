SELECT channel, unique_statuss FROM (SELECT channel, COUNT(DISTINCT shipping_fee) AS unique_statuss FROM orders WHERE status = 'SHIPPED' GROUP BY channel) AS s WHERE unique_statuss > 9
