WITH scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY revenue, id) seq_no,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY units DESC, id) seq_no_2,
        SUM(units) OVER (ORDER BY revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_units,
        LEAD(units, 2, 0) OVER (PARTITION BY store_id ORDER BY sale_date, id) next_units,
        RANK() OVER (ORDER BY units NULLS LAST) units_rank
    FROM store_sales
)
SELECT
    id,
    store_id,
    product_id,
    sale_date,
    units,
    revenue,
    seq_no,
    seq_no_2,
    cumulative_units
FROM scored
ORDER BY seq_no_2 DESC, id
