SELECT *, RANK() OVER (PARTITION BY country, city ORDER BY rating DESC NULLS FIRST) AS rank_in_group FROM suppliers QUALIFY rank_in_group = 1;
