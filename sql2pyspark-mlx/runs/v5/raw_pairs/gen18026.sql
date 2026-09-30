WITH base AS (
    SELECT *, ADD_MONTHS(sale_date, 6) sale_date_plus_months
    FROM store_sales
    WHERE store_id IN (2, 3, 4, 5)
),
deduped AS (
    SELECT
        id,
        store_id,
        sale_date,
        units,
        revenue
    FROM base
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY units DESC, id) = 1
),
latest AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY units, id) AS latest_rank
        FROM deduped
    ) x
    WHERE latest_rank = 1
),
filtered AS (
    SELECT
        id,
        revenue,
        latest_rank,
        CASE WHEN units >= 39 THEN 'High' WHEN units >= 17 THEN 'Medium' ELSE 'Low' END units_band
    FROM latest
    WHERE latest_rank >= 1
),
filtered_store_sales AS (
    SELECT *, latest_rank - revenue latest_rank_minus_revenue
    FROM filtered
    WHERE revenue BETWEEN 2200 AND 2300
),
scored AS (
    SELECT
        id,
        revenue,
        units_band,
        latest_rank_minus_revenue,
        AVG(revenue) OVER (ORDER BY latest_rank, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) revenue_avg_3,
        FIRST_VALUE(latest_rank_minus_revenue, TRUE) OVER (ORDER BY revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_latest_rank_minus_revenue,
        DENSE_RANK() OVER (ORDER BY latest_rank_minus_revenue DESC NULLS LAST) latest_rank_minus_revenue_dense_rank,
        ROW_NUMBER() OVER (ORDER BY revenue, id) event_seq
    FROM filtered_store_sales
),
with_windows AS (
    SELECT
        id,
        revenue_avg_3,
        first_known_latest_rank_minus_revenue,
        ROW_NUMBER() OVER w_part event_seq_2,
        DENSE_RANK() OVER (ORDER BY event_seq NULLS LAST) event_seq_dense_rank
    FROM scored
    WINDOW w_part AS (ORDER BY first_known_latest_rank_minus_revenue DESC, id)
)
SELECT
    id,
    revenue_avg_3,
    event_seq_dense_rank
FROM with_windows
ORDER BY event_seq_dense_rank;
