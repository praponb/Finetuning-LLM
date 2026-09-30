WITH base AS (
    SELECT
        *,
        ROUND(stock_qty, 1) AS stock_qty_rounded,
        stock_qty + price AS stock_qty_plus_price
    FROM products
    WHERE brand IS NOT NULL
),
enriched AS (
    SELECT
        a.id,
        a.product_name,
        a.price,
        a.cost,
        a.launch_date,
        a.supplier_id,
        a.stock_qty_plus_price,
        d.city,
        d.rating,
        d.supplier_name
    FROM base a
    INNER JOIN suppliers d
        ON a.supplier_id = d.id
    WHERE d.country = 'US'
),
joined AS (
    SELECT
        a.*,
        b.supplier_name AS supplier_supplier_name,
        b.rating AS supplier_rating
    FROM enriched a
    LEFT JOIN suppliers b
        ON a.supplier_id = b.id
),
filtered AS (
    SELECT
        id,
        product_name,
        price,
        cost,
        launch_date,
        stock_qty_plus_price,
        city,
        rating,
        supplier_supplier_name,
        TRIM(supplier_name) AS clean_supplier_name,
        CAST(cost AS INT) AS cost_int
    FROM joined
    WHERE city IS NOT NULL
      AND price >= 360
),
base_products AS (
    SELECT
        id,
        product_name,
        cost,
        launch_date,
        city,
        rating,
        supplier_supplier_name,
        price + cost_int AS price_plus_cost_int
    FROM filtered
    WHERE city IS NOT NULL
      AND cost_int < 36
),
deduped AS (
    SELECT
        id,
        city,
        supplier_supplier_name,
        price_plus_cost_int
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating DESC, id) AS latest_rank
        FROM base_products
    ) t
    WHERE latest_rank = 1
)
SELECT
    id,
    supplier_supplier_name,
    price_plus_cost_int,
    SUM(price_plus_cost_int) OVER (PARTITION BY city ORDER BY price_plus_cost_int, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_price_plus_cost_int,
    AVG(price_plus_cost_int) OVER (PARTITION BY city ORDER BY price_plus_cost_int, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS price_plus_cost_int_avg_2,
    NTILE(4) OVER (PARTITION BY city ORDER BY price_plus_cost_int, id) AS price_plus_cost_int_bucket
FROM deduped;
