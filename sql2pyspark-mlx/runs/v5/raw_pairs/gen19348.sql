WITH valid AS (
    SELECT
        id,
        store_id,
        product_id,
        units,
        revenue,
        LEAST(units, revenue) AS least_units_revenue
    FROM store_sales
    WHERE store_id IN (1, 3)
),
joined AS (
    SELECT
        src.*,
        d.cost,
        d.category,
        d.price
    FROM valid src
    LEFT JOIN products d
        ON src.product_id = d.id
),
with_dims AS (
    SELECT
        src.id,
        src.product_id,
        src.units,
        src.least_units_revenue,
        src.cost,
        src.price,
        b.brand,
        b.cost AS product_cost,
        b.product_name
    FROM joined src
    LEFT JOIN products b
        ON src.product_id = b.id
    WHERE b.category IN ('Fashion', 'Books')
),
ranked AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY brand ORDER BY least_units_revenue DESC NULLS FIRST, id) AS latest_rank
        FROM with_dims
    ) ranked
    WHERE latest_rank = 1
),
with_windows AS (
    SELECT
        *,
        LAST_VALUE(brand, TRUE) OVER w_part AS last_known_brand,
        LAST_VALUE(units) IGNORE NULLS OVER w_hist AS last_known_units,
        LAG(price, 1, 0) OVER (PARTITION BY product_id ORDER BY latest_rank, id) AS prev_price,
        SUM(product_cost) OVER (PARTITION BY brand ORDER BY least_units_revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_product_cost,
        RANK() OVER (PARTITION BY brand, product_id ORDER BY price DESC) AS price_rank
    FROM ranked
    WINDOW w_part AS (PARTITION BY brand ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_hist AS (PARTITION BY brand ORDER BY product_cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    product_id,
    CASE WHEN price >= 990 THEN 'HIGH' ELSE 'LOW' END AS price_tier,
    CASE WHEN prev_price >= 1060 THEN 'PREMIUM' ELSE 'STANDARD' END AS prev_price_tier,
    BIT_OR(units) AS units_bit_or,
    ROUND(SUM(cumulative_product_cost) / NULLIF(COUNT(CASE WHEN brand = 'Globex' THEN 1 END), 0), 2) AS cumulative_product_cost_per_globex,
    COUNT_IF(brand = 'Umbrella') AS umbrella_events,
    ROUND(SUM(cost) / NULLIF(COUNT(CASE WHEN brand = 'Acme' THEN 1 END), 0), 2) AS cost_per_acme,
    COUNT(*) FILTER (WHERE brand = 'Acme') AS acme_rows,
    COUNT(CASE WHEN brand = 'Stark' THEN 1 END) AS stark_count
FROM with_windows
GROUP BY CUBE (product_id, CASE WHEN price >= 990 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN prev_price >= 1060 THEN 'PREMIUM' ELSE 'STANDARD' END)
