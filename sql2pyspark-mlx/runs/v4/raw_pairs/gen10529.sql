WITH joined AS (
    SELECT
        src.*,
        b.price,
        b.category,
        b.cost
    FROM store_sales src
    LEFT JOIN products b
        ON src.product_id = b.id
),
joined_store_sales AS (
    SELECT
        s.*,
        p.price AS product_price,
        p.stock_qty
    FROM joined s
    LEFT JOIN products p
        ON s.product_id = p.id
),
filtered AS (
    SELECT *, ROUND(price / revenue, 2) AS price_per_revenue
    FROM joined_store_sales
    WHERE sale_date >= DATE '2023-09-07'
),
with_windows AS (
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        price,
        category,
        cost,
        product_price,
        stock_qty,
        price_per_revenue,
        AVG(revenue) OVER (PARTITION BY category ORDER BY stock_qty ASC, id ASC ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS revenue_avg_5,
        CORR(units, revenue) OVER (PARTITION BY store_id) AS corr_units_revenue,
        ROW_NUMBER() OVER (PARTITION BY category ORDER BY units DESC, id ASC) AS event_seq
    FROM filtered
)
SELECT
    *,
    ROUND(units / SUM(units) OVER (PARTITION BY store_id), 4) AS units_share,
    AVG(revenue_avg_5) OVER (PARTITION BY category ORDER BY product_price ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS revenue_avg_5_avg_2,
    SUM(cost) OVER (PARTITION BY product_id ORDER BY revenue ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_cost
FROM with_windows
