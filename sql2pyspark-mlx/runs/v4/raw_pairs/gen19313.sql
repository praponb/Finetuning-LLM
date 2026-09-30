WITH with_dims AS (
    SELECT
        s.*,
        b.sqft,
        b.region,
        b.opened_date
    FROM store_sales s
    LEFT JOIN stores b
        ON s.store_id = b.id
),
joined AS (
    SELECT
        e.*,
        p.category,
        p.stock_qty,
        p.cost
    FROM with_dims e
    LEFT JOIN products p
        ON e.product_id = p.id
),
with_dims_store_sales AS (
    SELECT
        s.*,
        p.store_name,
        p.city
    FROM joined s
    LEFT JOIN stores p
        ON s.store_id = p.id
),
enriched AS (
    SELECT
        a.*,
        b.region AS store_region,
        b.city AS store_city
    FROM with_dims_store_sales a
    INNER JOIN stores b
        ON a.store_id = b.id
),
with_windows AS (
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        sqft,
        region,
        opened_date,
        category,
        stock_qty,
        cost,
        store_name,
        city,
        store_region,
        store_city,
        SUM(stock_qty) OVER (PARTITION BY store_city ORDER BY revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_stock_qty,
        LAG(revenue, 1) OVER (PARTITION BY city, category ORDER BY stock_qty, id) AS prev_revenue,
        AVG(units) OVER (PARTITION BY category ORDER BY stock_qty, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS units_avg_3_prior
    FROM enriched
),
ranked AS (
    SELECT *
    FROM with_windows
    QUALIFY RANK() OVER (PARTITION BY city, product_id ORDER BY cumulative_stock_qty DESC NULLS LAST) = 1
),
deduped AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY store_region ORDER BY sqft DESC NULLS LAST, id) AS rn
        FROM ranked
    ) ranked
    WHERE rn = 1
)
SELECT
    id,
    store_id,
    sale_date,
    region,
    stock_qty,
    cost,
    store_name,
    store_city,
    cumulative_stock_qty
FROM deduped
ORDER BY store_city, store_id
