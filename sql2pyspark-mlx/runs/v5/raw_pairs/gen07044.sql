WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT *, YEAR(last_restock) AS last_restock_year
    FROM inventory
    WHERE last_restock > TO_DATE('2023-05-10')
      AND quantity = 148
),
with_dims AS (
    -- Attach stores attributes
    SELECT
        e.id,
        e.store_id,
        e.product_id,
        e.last_restock,
        e.last_restock_year,
        d.city
    FROM cleaned e
    LEFT JOIN stores d
        ON e.store_id = d.id
),
joined AS (
    -- Attach stores attributes
    SELECT
        s.*,
        p2.store_name,
        p2.opened_date,
        p2.city AS store_city
    FROM with_dims s
    LEFT JOIN stores p2
        ON s.store_id = p2.id
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        DATEDIFF(last_restock, opened_date) AS days_opened_date_to_last_restock,
        CASE WHEN last_restock_year >= 76 THEN 'High' WHEN last_restock_year >= 34 THEN 'Medium' ELSE 'Low' END AS last_restock_year_tier,
        COALESCE(store_city, 'UNKNOWN') AS store_city_filled
    FROM joined
    WHERE city IS DISTINCT FROM 'Tokyo'
      AND last_restock BETWEEN '2025-06-01' AND '2025-07-27'
)
-- Join a dimension and aggregate
SELECT
    dim.city AS store_city,
    e.city AS inventory_city,
    COUNT(*) AS num_rows,
    ROUND(AVG(e.days_opened_date_to_last_restock), 2) AS avg_days_opened_date_to_last_restock,
    GROUPING_ID() AS gid
FROM valid e
JOIN stores dim ON e.store_id = dim.id
GROUP BY GROUPING SETS ((dim.city, e.city), (dim.city), ());
