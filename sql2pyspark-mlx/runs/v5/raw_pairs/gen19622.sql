SELECT
    id,
    city,
    opened_date,
    region
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY opened_date DESC, id) AS rnk
    FROM stores
) AS r
WHERE rnk <= 3
ORDER BY region DESC, id DESC
