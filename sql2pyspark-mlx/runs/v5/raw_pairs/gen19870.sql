WITH cleaned AS (
    SELECT
        *,
        CONCAT(store_name, ' ', city) AS store_name_city,
        FLOOR(sqft) AS sqft_whole,
        COALESCE(region, 'N/A') AS region_clean
    FROM stores
    WHERE opened_date > DATE '2023-06-20'
      AND region IS NOT NULL
)
SELECT city, id
FROM cleaned
EXCEPT
SELECT city, id
FROM stores
WHERE store_name LIKE '%Mal%'
