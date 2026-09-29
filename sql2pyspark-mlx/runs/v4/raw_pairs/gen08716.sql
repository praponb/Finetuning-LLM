WITH joined AS (
    -- Attach stores attributes
    SELECT
        s.*,
        p.opened_date,
        p.city,
        p.store_name
    FROM inventory s
    INNER JOIN stores p
        ON s.store_id = p.id
)
-- Grouped metrics
SELECT
    CASE WHEN quantity >= 245 THEN 'LARGE' ELSE 'SMALL' END AS quantity_tier,
    DATE_TRUNC('MONTH', opened_date) AS activity_month,
    STDDEV_SAMP(quantity) AS quantity_stddev,
    ROUND(SUM(quantity) / NULLIF(COUNT(CASE WHEN city = 'Berlin' THEN 1 END), 0), 2) AS quantity_per_berlin,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list_2
FROM joined
GROUP BY CASE WHEN quantity >= 245 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('MONTH', opened_date)
ORDER BY quantity_stddev
