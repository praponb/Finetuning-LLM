WITH valid AS (
    SELECT
        id,
        product_id,
        sale_date,
        units,
        revenue,
        units * revenue AS revenue_per_unit,
        QUARTER(sale_date) AS sale_date_quarter
    FROM store_sales
    WHERE revenue <= 1800
)
SELECT
    id,
    product_id,
    sale_date,
    units,
    sale_date_quarter
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY revenue DESC, id) AS row_num
    FROM valid
) t
WHERE row_num = 1
