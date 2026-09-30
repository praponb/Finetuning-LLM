WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        GREATEST(units, revenue) AS greatest_units_revenue,
        DATEDIFF(CURRENT_DATE(), sale_date) AS days_since_sale_date
    FROM store_sales
    WHERE sale_date > DATE '2023-08-23'
      AND units > 36
),
enriched AS (
    -- Attach products attributes
    SELECT
        e.*,
        p.cost,
        p.stock_qty
    FROM base e
    LEFT JOIN products p
        ON e.product_id = p.id
),
enriched_store_sales AS (
    -- Ranking and gap features
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        greatest_units_revenue,
        days_since_sale_date,
        cost,
        stock_qty,
        NTILE(5) OVER (PARTITION BY store_id ORDER BY units DESC, id) AS units_bucket,
        STDDEV_SAMP(stock_qty) OVER w_part AS product_id_stddev_samp_stock_qty,
        ROW_NUMBER() OVER (PARTITION BY product_id, store_id ORDER BY units DESC, id) AS event_seq,
        FIRST_VALUE(greatest_units_revenue) IGNORE NULLS OVER (ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_greatest_units_revenue,
        AVG(greatest_units_revenue) OVER (PARTITION BY store_id ORDER BY days_since_sale_date, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS greatest_units_revenue_avg_2_prior
    FROM enriched
    WINDOW w_part AS (PARTITION BY product_id)
),
joined AS (
    -- Attach stores attributes
    SELECT
        e.*,
        b.city,
        b.opened_date
    FROM enriched_store_sales e
    INNER JOIN stores b
        ON e.store_id = b.id
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY greatest_units_revenue DESC, id) AS rank_in_group
        FROM joined
    ) ranked
    WHERE rank_in_group = 1
),
enriched_store_sales_2 AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(cost) OVER w_hist AS cost_avg_5_prior,
        LAST_VALUE(greatest_units_revenue_avg_2_prior) IGNORE NULLS OVER w_running AS last_known_greatest_units_revenue_avg_2_prior,
        NTILE(3) OVER (ORDER BY greatest_units_revenue_avg_2_prior, id) AS greatest_units_revenue_avg_2_prior_bucket
    FROM ranked
    WINDOW w_hist AS (PARTITION BY city, store_id ORDER BY greatest_units_revenue_avg_2_prior, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING), w_running AS (PARTITION BY store_id, city ORDER BY revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('YEAR', sale_date) AS year_start,
    BIT_AND(units) AS units_bit_and,
    APPROX_PERCENTILE(stock_qty, ARRAY(0.25, 0.5, 0.75)) AS stock_qty_quartiles,
    ROUND(SUM(last_known_greatest_units_revenue_avg_2_prior) / NULLIF(COUNT(CASE WHEN city = 'Berlin' THEN 1 END), 0), 2) AS last_known_greatest_units_revenue_avg_2_prior_per_berlin,
    SUM(CASE WHEN city = 'Bangkok' THEN days_since_sale_date ELSE 0 END) AS bangkok_days_since_sale_date
FROM enriched_store_sales_2
GROUP BY DATE_TRUNC('YEAR', sale_date)
ORDER BY year_start DESC NULLS FIRST;
