WITH filtered AS (
    SELECT *, DATE_FORMAT(opened_date, 'MMM yyyy') opened_date_label
    FROM stores
    WHERE region IS NOT NULL
      AND store_name NOT LIKE '%Centr%'
)
SELECT region, id
FROM filtered
EXCEPT DISTINCT
SELECT region, id
FROM stores
WHERE store_name LIKE 'R%'
