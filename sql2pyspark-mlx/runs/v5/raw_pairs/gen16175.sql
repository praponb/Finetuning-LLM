WITH valid AS (
    SELECT
        id,
        product_id,
        sale_date,
        units,
        revenue,
        units + revenue AS revenue_per_unit
    FROM store_sales
    WHERE sale_date > TO_DATE('2023-02-17')
      AND store_id IN (1, 2)
)
SELECT id, product_id
FROM valid
INTERSECT
SELECT id, product_id
FROM store_sales
WHERE product_id IN (2, 10);
