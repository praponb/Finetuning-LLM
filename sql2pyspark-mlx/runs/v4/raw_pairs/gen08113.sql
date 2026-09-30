WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        MONTH(last_restock) AS last_restock_month,
        ADD_MONTHS(last_restock, 12) AS last_restock_plus_months
    FROM inventory
    WHERE last_restock > DATE '2024-01-09'
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity DESC NULLS LAST, id ASC) AS rn
    FROM filtered
    QUALIFY rn = 1
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LAST_DAY(last_restock) AS last_restock_month_end
    FROM latest
    WHERE product_id IN (2, 5, 9)
),
joined AS (
    -- Attach stores attributes
    SELECT
        e.*,
        s.sqft,
        s.store_name
    FROM cleaned e
    LEFT JOIN stores s
        ON e.store_id = s.id
)
SELECT
    s.id,
    s.store_id,
    s.quantity,
    s.last_restock,
    s.last_restock_month,
    s.last_restock_plus_months,
    s.sqft,
    s.store_name,
    CASE WHEN store_name RLIKE '^(Riverside|Old Town)$' THEN 'OTHER_ZONE' ELSE 'PRIORITY' END AS store_name_zone,
    (SELECT SUM(t2.revenue) FROM store_sales t2 WHERE t2.store_id = s.store_id) AS sum_store_sale_revenue
FROM joined s
ORDER BY last_restock DESC;
