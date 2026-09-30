WITH valid AS (
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        CASE WHEN quantity >= 182 THEN 'High' WHEN quantity >= 41 THEN 'Medium' ELSE 'Low' END AS quantity_band
    FROM inventory
    WHERE last_restock > DATE '2023-12-30'
),
joined AS (
    SELECT
        e.*,
        p.opened_date,
        p.city,
        p.sqft
    FROM valid e
    LEFT JOIN stores p
        ON e.store_id = p.id
),
filtered AS (
    SELECT *, DAYOFWEEK(opened_date) AS opened_date_dayofweek
    FROM joined
    WHERE last_restock >= DATE '2024-02-22'
      AND city IS NOT NULL
)
SELECT
    CASE WHEN quantity >= 235 THEN 'HIGH' ELSE 'LOW' END AS quantity_tier,
    COUNT(CASE WHEN city = 'London' THEN 1 END) AS london_count,
    COUNT(*) FILTER (WHERE city = 'New York') AS new_york_rows,
    ROUND(AVG(sqft), 2) AS avg_sqft,
    SIZE(COLLECT_SET(city)) AS num_cities
FROM filtered
GROUP BY 1
ORDER BY num_cities DESC NULLS LAST;
