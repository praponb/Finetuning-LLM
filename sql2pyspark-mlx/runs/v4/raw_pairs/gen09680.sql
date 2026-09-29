WITH valid AS (
    SELECT
        id,
        product_id,
        units,
        revenue,
        ADD_MONTHS(sale_date, 12) AS sale_date_plus_months
    FROM store_sales
    WHERE sale_date > DATE '2024-05-01'
),
with_windows AS (
    SELECT
        *,
        LAG(units, 1) OVER (ORDER BY units, id) AS prev_units,
        CORR(units, revenue) OVER (PARTITION BY product_id) AS corr_units_revenue,
        SUM(units) OVER (ORDER BY units, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_units,
        FIRST_VALUE(revenue) IGNORE NULLS OVER (PARTITION BY product_id ORDER BY sale_date_plus_months, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_revenue
    FROM valid
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY units DESC NULLS LAST, id) AS dedup_rank
    FROM with_windows
    QUALIFY dedup_rank = 1
),
enriched AS (
    SELECT
        a.*,
        p.launch_date,
        p.category,
        p.price
    FROM ranked a
    LEFT JOIN products p
        ON a.product_id = p.id
),
metrics AS (
    SELECT
        *,
        LAG(prev_units, 1) OVER (PARTITION BY product_id, category ORDER BY revenue, id) AS prev_prev_units,
        SUM(units) OVER (PARTITION BY category ORDER BY dedup_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_units_2,
        NTILE(5) OVER (PARTITION BY product_id ORDER BY first_known_revenue, id) AS first_known_revenue_bucket,
        ROUND(revenue / SUM(revenue) OVER (PARTITION BY category), 4) AS revenue_share
    FROM enriched
),
enriched_store_sales AS (
    SELECT a.*, b.price AS product_price
    FROM metrics a
    INNER JOIN products b
        ON a.product_id = b.id
),
latest AS (
    SELECT *
    FROM enriched_store_sales
    QUALIFY ROW_NUMBER() OVER (PARTITION BY category, product_id ORDER BY revenue, id) = 1
)
SELECT *
FROM (
    SELECT
        id,
        category,
        corr_units_revenue,
        first_known_revenue,
        price
    FROM latest
) src
UNPIVOT (
    reading FOR measure IN (corr_units_revenue AS CORR_UNITS_REVENUE_VAL, first_known_revenue AS FIRST_KNOWN_REVENUE, price AS PRICE)
);
