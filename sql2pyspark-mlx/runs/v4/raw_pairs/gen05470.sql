WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        INITCAP(TRIM(category)) AS category_title,
        UPPER(TRIM(product_name)) AS product_name_normalized
    FROM products
    WHERE supplier_id IS NOT NULL
      AND MONTH(launch_date) IN (3, 5, 7)
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY launch_date, id) AS event_seq,
        AVG(cost) OVER (ORDER BY cost, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS cost_avg_6,
        AVG(stock_qty) OVER w_hist AS stock_qty_avg_6_prior
    FROM cleaned
    WINDOW w_hist AS (PARTITION BY supplier_id, category ORDER BY stock_qty, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING)
),
with_dims AS (
    -- Attach suppliers attributes
    SELECT
        src.*,
        d.city,
        d.rating,
        d.supplier_name
    FROM scored src
    INNER JOIN suppliers d
        ON src.supplier_id = d.id
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        city,
        rating,
        price,
        cost
    FROM with_dims
) src
UNPIVOT (
    reading FOR metric IN (rating, price, cost)
);
