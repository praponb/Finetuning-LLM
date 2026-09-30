SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY discount_pct, id) AS latest_rank
    FROM promotions
) ranked
WHERE latest_rank = 1;
