SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating DESC NULLS FIRST, id) AS latest_rank FROM suppliers QUALIFY latest_rank = 1
