-- Q04c: GROUPING SETS / ROLLUP / CUBE with grouping_id and FILTER clause
-- needs setup views: orders
-- @id: Q04c
SELECT o.channel, o.status, COUNT(*) AS orders
FROM orders o
GROUP BY CUBE (o.channel, o.status)
HAVING COUNT(*) > 1
ORDER BY channel NULLS FIRST, status NULLS FIRST;
