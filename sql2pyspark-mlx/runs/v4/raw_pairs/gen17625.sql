WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        DATE_ADD(opened_date, 64) AS opened_date_plus_64d,
        DATE_FORMAT(opened_date, 'MMM yyyy') AS opened_date_label
    FROM stores
    WHERE opened_date >= TO_DATE('2024-05-03')
      AND region IS NOT NULL
)
-- Deduplicate with ROW_NUMBER
SELECT *
FROM (
    SELECT *, RANK() OVER (PARTITION BY region ORDER BY opened_date DESC NULLS FIRST) AS rank_in_group
    FROM cleaned
) t
WHERE rank_in_group = 1;
