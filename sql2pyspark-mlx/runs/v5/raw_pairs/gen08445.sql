WITH valid AS (
    SELECT *, FLOOR(cost) AS cost_whole
    FROM products
    WHERE launch_date >= DATE '2023-05-24'
),
with_latest AS (
    SELECT
        s.id,
        s.product_name,
        s.category,
        s.brand,
        s.price,
        s.launch_date,
        s.tags,
        x.order_item_unit_price
    FROM valid s
    JOIN LATERAL (
            SELECT c.unit_price AS order_item_unit_price
            FROM order_items c
            WHERE c.product_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
base AS (
    SELECT *, YEAR(launch_date) AS launch_date_year
    FROM with_latest
    WHERE brand IS NOT NULL
),
base_products AS (
    SELECT *, DATEDIFF(CURRENT_DATE(), launch_date) AS days_since_launch_date
    FROM base
    WHERE launch_date >= DATE '2023-03-26'
),
scored AS (
    SELECT
        *,
        AVG(order_item_unit_price) OVER (ORDER BY days_since_launch_date, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS order_item_unit_price_avg_3_prior,
        NTILE(10) OVER (PARTITION BY brand ORDER BY price, id) AS price_bucket,
        SUM(launch_date_year) OVER (PARTITION BY brand, category ORDER BY price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_launch_date_year
    FROM base_products
    WHERE days_since_launch_date <> 30
)
SELECT
    id,
    brand,
    measure,
    val
FROM (
    SELECT
        id,
        brand,
        price,
        order_item_unit_price
    FROM scored
) src
UNPIVOT (
    val FOR measure IN (price AS PRICE, order_item_unit_price AS ORDER_ITEM_UNIT_PRICE)
)
