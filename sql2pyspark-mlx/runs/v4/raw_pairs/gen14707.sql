WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        city,
        region,
        opened_date,
        LOWER(region) AS region_lower,
        CEIL(sqft) AS sqft_whole,
        COALESCE(region, 'DIRECT') AS region_filled
    FROM stores
    WHERE opened_date >= TO_DATE('2023-11-15')
      AND sqft BETWEEN 12841 AND 14129
)
-- Semi-join via a correlated subquery
SELECT s.*
FROM base s
WHERE EXISTS (
        SELECT 1
        FROM orders c
        WHERE c.store_id = s.id
          AND c.amount >= 1600
    );
