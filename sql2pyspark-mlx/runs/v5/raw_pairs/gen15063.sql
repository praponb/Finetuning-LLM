WITH base AS (
    SELECT
        id,
        store_id,
        product_id,
        revenue,
        ROUND(units, 1) AS units_rounded,
        COALESCE(revenue, 0) AS revenue_filled
    FROM store_sales
    WHERE store_id IN (4, 5)
),
deduped AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY units_rounded DESC NULLS FIRST, id) AS rn
        FROM base
    ) t
    WHERE rn = 1
),
joined AS (
    SELECT
        a.id,
        a.store_id,
        a.product_id,
        a.units_rounded,
        a.rn,
        b.region,
        b.store_name,
        b.city
    FROM deduped a
    LEFT JOIN stores b
        ON a.store_id = b.id
),
with_dims AS (
    SELECT
        a.*,
        p.price,
        p.brand
    FROM joined a
    LEFT JOIN products p
        ON a.product_id = p.id
),
remaining_keys AS (
    SELECT store_id
    FROM with_dims
    INTERSECT
    SELECT store_id
    FROM store_sales
    WHERE units IS NOT NULL
),
reconciled AS (
    SELECT s.*
    FROM with_dims s
    JOIN remaining_keys k ON s.store_id = k.store_id
),
base_store_sales AS (
    SELECT
        product_id,
        units_rounded,
        rn,
        region,
        city,
        price,
        brand,
        price - units_rounded AS price_minus_units_rounded,
        TRIM(city) AS clean_city
    FROM reconciled
    WHERE city = 'Bangkok'
),
valid AS (
    SELECT
        product_id,
        units_rounded,
        rn,
        city,
        price,
        brand,
        clean_city,
        UPPER(region) AS region_upper,
        COALESCE(city, 'DIRECT') AS resolved_city
    FROM base_store_sales
    WHERE product_id IN (6, 9)
)
SELECT
    s.product_id,
    s.rn,
    s.city,
    s.price,
    s.brand,
    s.clean_city,
    s.resolved_city,
    (SELECT SUM(t2.revenue) FROM store_sales t2 WHERE t2.product_id = s.product_id) AS sum_store_sale_revenue
FROM valid s
ORDER BY rn DESC NULLS FIRST
