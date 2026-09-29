WITH valid AS (
    SELECT
        *,
        LOWER(city) AS city_lower,
        ROUND(sqft, 1) AS sqft_rounded
    FROM stores
    WHERE region IS NOT NULL
)
SELECT *, ROW_NUMBER() OVER (PARTITION BY city, region ORDER BY sqft DESC NULLS LAST, id ASC) AS rn
FROM valid
QUALIFY rn = 1;
