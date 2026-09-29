WITH latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY units, id) AS rn
        FROM store_sales
    ) t
    WHERE rn = 1
),
with_dims AS (
    -- Attach stores attributes
    SELECT
        a.*,
        s.opened_date,
        s.city,
        s.region
    FROM latest a
    INNER JOIN stores s
        ON a.store_id = s.id
),
enriched AS (
    -- Attach products attributes
    SELECT e.*, p.brand
    FROM with_dims e
    LEFT JOIN products p
        ON e.product_id = p.id
),
joined AS (
    -- Attach stores attributes
    SELECT
        s.*,
        d.opened_date AS store_opened_date,
        d.store_name
    FROM enriched s
    LEFT JOIN stores d
        ON s.store_id = d.id
)
SELECT
    id,
    product_id,
    units,
    revenue,
    city,
    brand,
    store_name
FROM joined
ORDER BY city
