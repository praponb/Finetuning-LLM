SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY quantity ASC, id ASC) AS dedup_rank
    FROM inventory
) x
WHERE dedup_rank = 1;
