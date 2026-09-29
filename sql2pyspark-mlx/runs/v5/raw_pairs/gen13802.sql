WITH cleaned AS (
    SELECT
        id,
        sale_date,
        units,
        revenue,
        GREATEST(units, revenue) AS greatest_units_revenue
    FROM store_sales
    WHERE sale_date >= TO_DATE('2023-05-10')
      AND units IS NOT NULL
)
SELECT id
FROM cleaned
EXCEPT ALL
SELECT id
FROM store_sales
WHERE store_id IN (3, 4, 6);
