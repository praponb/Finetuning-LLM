SELECT
    id,
    country,
    rating
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating NULLS FIRST, id) AS latest_rank
    FROM suppliers
) ranked
WHERE latest_rank = 1;
