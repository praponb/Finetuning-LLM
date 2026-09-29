WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        CAST(sqft AS DOUBLE) AS sqft_num,
        COALESCE(region, 'NONE') AS region_filled
    FROM stores
    WHERE region IS NOT NULL
)
SELECT id
FROM valid
EXCEPT ALL
SELECT id
FROM stores
WHERE city = 'New York';
