SELECT *, RANK() OVER (PARTITION BY country ORDER BY rating DESC) AS rank_in_group
FROM suppliers
QUALIFY rank_in_group = 1
