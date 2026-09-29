WITH with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(revenue) OVER (PARTITION BY store_id ORDER BY revenue, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) revenue_avg_5_prior,
        ROUND(revenue / SUM(revenue) OVER (PARTITION BY store_id), 4) revenue_share,
        SUM(revenue) OVER (PARTITION BY product_id ORDER BY revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_revenue,
        AVG(units) OVER (PARTITION BY store_id ORDER BY revenue, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) units_avg_6_prior
    FROM store_sales
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(revenue_avg_5_prior * 1.07, 2) revenue_avg_5_prior_scaled,
        CAST(cumulative_revenue AS DOUBLE) cumulative_revenue_str
    FROM with_windows
    WHERE sale_date >= TO_DATE('2023-07-02')
      AND units = 16
),
joined AS (
    -- Attach products attributes
    SELECT
        e.*,
        b.price,
        b.product_name,
        b.brand
    FROM base e
    INNER JOIN products b
        ON e.product_id = b.id
)
SELECT
    id,
    product_id,
    sale_date,
    units,
    revenue,
    cumulative_revenue,
    units_avg_6_prior,
    cumulative_revenue_str,
    price
FROM joined
