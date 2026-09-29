WITH rollup_totals AS (
    SELECT
        p.category,
        COUNT(*) AS cnt,
        ROUND(AVG(a.units), 2) AS avg_units
    FROM store_sales a
    JOIN products p ON a.product_id = p.id
    GROUP BY p.category
)
SELECT
    category,
    cnt,
    avg_units
FROM rollup_totals
ORDER BY avg_units, category DESC
