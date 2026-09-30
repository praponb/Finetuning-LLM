WITH cleaned AS (
    SELECT *, CONCAT_WS('-', product_name, category) AS product_name_category_key
    FROM products
    WHERE supplier_id IS NOT NULL
      AND supplier_id IN (1, 10, 11, 18)
),
ranked AS (
    SELECT *
    FROM cleaned
    QUALIFY ROW_NUMBER() OVER (PARTITION BY brand ORDER BY cost DESC, id) = 1
),
latest AS (
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY category ORDER BY launch_date DESC) AS row_num
        FROM ranked
    ) t
    WHERE row_num = 1
),
pivoted AS (
    SELECT *
    FROM (
        SELECT
            supplier_id,
            brand,
            price
        FROM latest
    ) src
    PIVOT (
        MAX(price)
        FOR brand IN ('Initech', 'Umbrella', 'Globex')
    )
)
SELECT
    supplier_id,
    Initech,
    Globex
FROM pivoted
ORDER BY supplier_id DESC, Globex;
