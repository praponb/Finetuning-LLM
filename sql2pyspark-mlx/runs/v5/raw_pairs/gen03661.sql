WITH joined AS (
    -- Attach products attributes
    SELECT a.*, p.price
    FROM inventory a
    LEFT JOIN products p
        ON a.product_id = p.id
    WHERE p.launch_date BETWEEN '2023-06-03' AND '2024-03-01'
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM joined
    QUALIFY ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY quantity NULLS FIRST, id) = 1
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(quantity) OVER (PARTITION BY store_id, product_id ORDER BY price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_quantity,
        MIN(quantity) OVER (PARTITION BY store_id, product_id ORDER BY price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_quantity,
        SUM(quantity) OVER (PARTITION BY store_id, product_id ORDER BY price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_quantity,
        LAST_VALUE(quantity, TRUE) OVER (ORDER BY price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_quantity
    FROM latest
),
enriched AS (
    -- Attach stores attributes
    SELECT
        e.*,
        b.city,
        b.sqft,
        b.region
    FROM metrics e
    JOIN stores b
        ON e.store_id = b.id
)
SELECT
    s.id,
    s.store_id,
    s.product_id,
    s.quantity,
    s.last_restock,
    s.price,
    s.running_max_quantity,
    s.city,
    s.sqft,
    CASE WHEN city RLIKE '^(Chiang Mai|London)$' THEN 'GROUP_B' ELSE 'PRIORITY' END AS city_zone,
    (SELECT MIN(x.units) FROM store_sales x WHERE x.product_id = s.product_id) AS min_store_sale_units
FROM enriched s
ORDER BY price;
