WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        product_id,
        sale_date,
        units,
        revenue,
        LAST_DAY(sale_date) AS sale_date_month_end
    FROM store_sales
    WHERE sale_date >= TO_DATE('2024-01-26')
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (ORDER BY revenue DESC) AS revenue_rank,
        SUM(revenue) OVER (ORDER BY sale_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_revenue,
        LAG(units, 1) OVER (ORDER BY revenue, id) AS prev_units,
        ROUND(revenue / SUM(revenue) OVER (PARTITION BY product_id), 4) AS revenue_share,
        CUME_DIST() OVER (PARTITION BY product_id ORDER BY revenue NULLS LAST) AS revenue_cume_dist
    FROM filtered
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        product_id,
        sale_date,
        units,
        revenue,
        sale_date_month_end,
        revenue_rank,
        running_revenue,
        prev_units,
        revenue_share,
        revenue_cume_dist,
        ROW_NUMBER() OVER w_ordered AS seq_no,
        ROW_NUMBER() OVER w_ordered AS row_seq,
        SUM(revenue_cume_dist) OVER w_recent AS running_revenue_cume_dist,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY sale_date DESC, id) AS event_seq,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY revenue_rank, id) AS event_seq_2
    FROM scored
    WINDOW w_ordered AS (PARTITION BY product_id ORDER BY running_revenue DESC, id), w_recent AS (PARTITION BY product_id ORDER BY running_revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        product_id,
        revenue_share,
        revenue_cume_dist,
        revenue
    FROM enriched
) src
UNPIVOT INCLUDE NULLS (
    value FOR metric IN (revenue_share, revenue_cume_dist, revenue)
)
