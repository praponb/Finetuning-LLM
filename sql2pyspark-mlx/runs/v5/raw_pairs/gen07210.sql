WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_id,
        product_id,
        units,
        revenue,
        ROUND(units / revenue, 2) revenue_per_unit,
        CEIL(units) units_whole
    FROM store_sales
    WHERE sale_date >= DATE '2023-08-15'
      AND units < 11
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY revenue DESC, id) seq_no,
        NTILE(3) OVER (PARTITION BY store_id, product_id ORDER BY units_whole, id) units_whole_bucket
    FROM filtered
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_id,
        revenue,
        revenue_per_unit,
        units_whole,
        seq_no,
        MAX(revenue_per_unit) OVER (PARTITION BY store_id ORDER BY seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_max_revenue_per_unit,
        COUNT(revenue_per_unit) OVER (PARTITION BY store_id ORDER BY seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_count_revenue_per_unit,
        MIN(revenue_per_unit) OVER (PARTITION BY store_id ORDER BY seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_min_revenue_per_unit,
        SUM(revenue_per_unit) OVER (PARTITION BY store_id ORDER BY seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_sum_revenue_per_unit
    FROM with_windows
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY revenue DESC NULLS LAST, id) AS latest_rank
        FROM scored
    ) ranked
    WHERE latest_rank = 1
)
SELECT
    id,
    store_id,
    revenue_per_unit,
    seq_no,
    running_max_revenue_per_unit,
    running_min_revenue_per_unit,
    running_sum_revenue_per_unit,
    latest_rank
FROM deduped
