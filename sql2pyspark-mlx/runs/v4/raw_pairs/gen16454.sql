WITH cleaned AS (
    SELECT
        id,
        product_name,
        category,
        brand,
        price,
        cost,
        stock_qty,
        supplier_id,
        ADD_MONTHS(launch_date, 1) AS launch_date_plus_months
    FROM products
    WHERE supplier_id IS NOT NULL
      AND category = 'Home'
),
enriched AS (
    SELECT
        s.*,
        p2.rating,
        p2.city,
        p2.country
    FROM cleaned s
    LEFT JOIN suppliers p2
        ON s.supplier_id = p2.id
),
deduped AS (
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY category ORDER BY stock_qty DESC NULLS FIRST) AS row_num
        FROM enriched
    ) x
    WHERE row_num = 1
),
ranked AS (
    SELECT *
    FROM deduped
    QUALIFY ROW_NUMBER() OVER (PARTITION BY category, city ORDER BY row_num DESC, id) = 1
)
SELECT
    *,
    NTILE(3) OVER (PARTITION BY category ORDER BY price, id) AS price_bucket,
    LAST_VALUE(country, TRUE) OVER (PARTITION BY supplier_id ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_country,
    CUME_DIST() OVER (ORDER BY cost DESC NULLS LAST) AS cost_cume_dist,
    ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating, id) AS row_seq
FROM ranked
