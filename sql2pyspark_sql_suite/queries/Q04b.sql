-- Q04b: ROLLUP and CUBE variants (converter should handle both shorthand forms)
-- needs setup views: customers, orders
-- @id: Q04b
SELECT c.country_code, o.channel, COUNT(DISTINCT o.order_id) AS orders, grouping_id() AS gid
FROM orders o JOIN customers c ON c.customer_id = o.customer_id
GROUP BY ROLLUP (c.country_code, o.channel)
ORDER BY gid, country_code, channel;
