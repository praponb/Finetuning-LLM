SELECT *
FROM inventory
QUALIFY RANK() OVER (PARTITION BY store_id ORDER BY last_restock NULLS LAST) = 1;
