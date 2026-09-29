WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        sale_date,
        revenue,
        CAST(revenue AS INT) AS revenue_int
    FROM store_sales
    WHERE sale_date > DATE '2023-04-01'
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, LAST_DAY(sale_date) AS sale_date_month_end
    FROM valid
    WHERE sale_date > DATE '2023-05-05'
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        sale_date,
        revenue,
        revenue_int,
        sale_date_month_end,
        LAG(revenue, 2) OVER (PARTITION BY store_id ORDER BY sale_date, id) AS prev_revenue,
        NTILE(10) OVER w_running AS revenue_bucket
    FROM cleaned
    WHERE revenue BETWEEN 2500 AND 3600
    WINDOW w_running AS (PARTITION BY store_id ORDER BY revenue DESC, id)
),
cleaned_store_sales AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        revenue,
        revenue_int,
        prev_revenue,
        revenue_bucket,
        DATE_TRUNC('WEEK', sale_date_month_end) AS sale_date_month_end_period,
        GREATEST(revenue_int, revenue) AS greatest_revenue_int_revenue
    FROM with_windows
    WHERE sale_date <= '2024-07-01'
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN prev_revenue >= 4100 THEN 'PREMIUM' ELSE 'STANDARD' END AS prev_revenue_tier,
    DATE_TRUNC('QUARTER', sale_date_month_end_period) AS quarter_start,
    MIN(sale_date_month_end_period) AS first_sale_date_month_end_period,
    MAX(sale_date_month_end_period) AS last_sale_date_month_end_period
FROM cleaned_store_sales
GROUP BY CASE WHEN prev_revenue >= 4100 THEN 'PREMIUM' ELSE 'STANDARD' END, DATE_TRUNC('QUARTER', sale_date_month_end_period);
