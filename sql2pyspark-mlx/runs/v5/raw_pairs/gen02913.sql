WITH cleaned AS (
    SELECT
        *,
        FLOOR(units) AS units_whole,
        DATE_FORMAT(sale_date, 'yyyy-MM') AS sale_date_label
    FROM store_sales
    WHERE sale_date >= DATE '2023-03-20'
      AND store_id IN (1, 2, 3, 6)
)
SELECT
    e.id,
    e.store_id,
    e.units,
    e.revenue,
    p.city,
    p.opened_date
FROM cleaned e
LEFT JOIN stores p
    ON e.store_id = p.id;
