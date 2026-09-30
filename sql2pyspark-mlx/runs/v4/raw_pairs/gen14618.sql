SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY opened_date DESC NULLS FIRST, id) AS rank_in_group
    FROM stores
) t
WHERE rank_in_group = 1;
