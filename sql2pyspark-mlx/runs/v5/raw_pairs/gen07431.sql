WITH src AS (
    SELECT *, ADD_MONTHS(sale_date, 6) AS sale_date_plus_months
    FROM store_sales
    WHERE units IS NULL OR units > 19
),
agg AS (
    SELECT store_id, MAX(revenue) - MIN(revenue) AS revenue_range
    FROM src
    GROUP BY store_id
)
SELECT store_id, revenue_range
FROM agg
ORDER BY store_id, revenue_range DESC
