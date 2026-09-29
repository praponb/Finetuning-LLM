SELECT
    sale_cohort,
    store_id,
    stddev_revenue,
    stddev_units
FROM (
    SELECT
        CASE WHEN YEAR(sale_date) < 2024 THEN 'before_2024' WHEN YEAR(sale_date) = 2024 THEN '2024' ELSE 'after_2024' END AS sale_cohort,
        store_id,
        ROUND(STDDEV(revenue), 2) AS stddev_revenue,
        ROUND(STDDEV(units), 2) AS stddev_units
    FROM store_sales
    WHERE product_id IN (4, 8)
    GROUP BY CASE WHEN YEAR(sale_date) < 2024 THEN 'before_2024' WHEN YEAR(sale_date) = 2024 THEN '2024' ELSE 'after_2024' END, store_id
) AS t
WHERE stddev_units >= 47;
