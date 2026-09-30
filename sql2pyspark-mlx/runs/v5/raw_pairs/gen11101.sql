WITH base AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(units / revenue, 2) AS revenue_per_unit
    FROM store_sales
    WHERE sale_date > DATE '2023-04-11'
      AND store_id IN (4, 6)
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER w_grp AS event_seq,
        SUM(units) OVER (ORDER BY revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_units
    FROM base
    WHERE MONTH(sale_date) IN (1, 2, 6)
    WINDOW w_grp AS (PARTITION BY store_id, product_id ORDER BY revenue_per_unit, id)
),
enriched_store_sales AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(cumulative_units) OVER w_grp AS prev_cumulative_units,
        LEAD(revenue) OVER w_grp AS next_revenue,
        LAG(revenue) OVER w_grp AS prev_revenue,
        ROW_NUMBER() OVER w_grp AS row_seq
    FROM enriched
    WINDOW w_grp AS (PARTITION BY store_id ORDER BY units, id)
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(prev_cumulative_units) OVER w_ordered AS running_prev_cumulative_units,
        ROW_NUMBER() OVER w_grp AS event_seq_2
    FROM enriched_store_sales
    WINDOW w_ordered AS (PARTITION BY product_id ORDER BY cumulative_units, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_grp AS (PARTITION BY product_id ORDER BY event_seq, id)
)
-- Running and moving aggregates
SELECT
    id,
    product_id,
    units,
    revenue,
    event_seq,
    prev_cumulative_units,
    prev_revenue,
    event_seq_2,
    ROUND(prev_revenue / SUM(prev_revenue) OVER (PARTITION BY store_id), 4) AS prev_revenue_share,
    AVG(running_prev_cumulative_units) OVER (PARTITION BY store_id ORDER BY cumulative_units, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS running_prev_cumulative_units_avg_5_prior
FROM metrics
