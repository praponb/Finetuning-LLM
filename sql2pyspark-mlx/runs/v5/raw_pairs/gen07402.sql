SELECT *
FROM (
    SELECT *, RANK() OVER (PARTITION BY city ORDER BY opened_date DESC NULLS LAST) AS latest_rank
    FROM stores
) t
WHERE latest_rank = 1;
