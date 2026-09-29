WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        UPPER(TRIM(supplier_name)) AS supplier_name_normalized,
        COALESCE(city, 'OTHER') AS city_filled
    FROM suppliers
    WHERE city IS NOT NULL
      AND LOWER(supplier_name) LIKE '%oky%'
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY city, country ORDER BY rating DESC, id ASC) AS latest_rank
        FROM base
    ) t
    WHERE latest_rank = 1
),
deduped_suppliers AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating DESC NULLS LAST, id ASC) AS row_num
    FROM deduped
    QUALIFY row_num = 1
),
with_latest AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.brand,
        top1.product_launch_date
    FROM deduped_suppliers s
    JOIN LATERAL (
            SELECT c.brand, c.launch_date AS product_launch_date
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.cost DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        city,
        brand,
        rating
    FROM with_latest
) src
PIVOT (
    MAX(rating) AS amount,
    COUNT(1) AS n
    FOR brand IN ('Initech' AS initech, 'Globex' AS globex)
);
