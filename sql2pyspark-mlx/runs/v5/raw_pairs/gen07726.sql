SELECT id, supplier_name, country, city FROM (SELECT *, RANK() OVER (PARTITION BY city ORDER BY rating NULLS LAST) AS dedup_rank FROM suppliers) ranked WHERE dedup_rank = 1
