SELECT *
FROM (
    SELECT *, DENSE_RANK() OVER (PARTITION BY store_id ORDER BY last_restock DESC) AS pos
    FROM inventory
) AS t
WHERE pos <= 5
ORDER BY 6 DESC
