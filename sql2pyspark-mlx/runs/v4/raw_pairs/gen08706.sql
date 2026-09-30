WITH base AS (
    SELECT
        id,
        store_name,
        city,
        region,
        sqft,
        LAST_DAY(opened_date) AS opened_date_month_end
    FROM stores
    WHERE opened_date >= TO_DATE('2024-02-23')
      AND region IS NOT NULL
      AND sqft IS NOT NULL
)
SELECT *
FROM (
    SELECT
        region,
        city,
        sqft
    FROM base
) src
PIVOT (
    MAX(sqft)
    FOR city IN ('Chiang Mai' AS chiang_mai, 'New York' AS new_york, 'Singapore' AS singapore)
);
