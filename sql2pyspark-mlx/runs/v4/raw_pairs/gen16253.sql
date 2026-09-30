SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY cost DESC, id ASC) AS rank_in_group
    FROM shipments
) x
WHERE rank_in_group = 1
