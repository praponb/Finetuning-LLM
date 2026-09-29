WITH rollup_totals AS (
    SELECT
        p.country,
        p.city,
        COUNT(*) AS cnt,
        MIN(a.cost) AS min_cost,
        ROUND(AVG(a.stock_qty), 2) AS avg_stock_qty,
        GROUPING_ID() AS gid
    FROM products a
    LEFT JOIN suppliers p ON a.supplier_id = p.id
    GROUP BY GROUPING SETS ((p.country, p.city), (p.country), ())
)
SELECT min_cost
FROM rollup_totals;
