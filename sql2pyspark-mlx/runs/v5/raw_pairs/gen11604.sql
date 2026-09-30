SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY region, city ORDER BY opened_date DESC, id ASC) AS rank_in_group
    FROM stores
) x
WHERE rank_in_group = 1
