SELECT id, supplier_name
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country, city ORDER BY rating DESC NULLS LAST, id) AS latest_rank
    FROM suppliers
) x
WHERE latest_rank = 1;
