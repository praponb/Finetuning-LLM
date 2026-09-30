WITH enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        product_id,
        sale_date,
        units,
        revenue,
        LEAD(revenue) OVER w_recent AS next_revenue,
        LAG(revenue) OVER w_recent AS prev_revenue,
        LAG(units) OVER w_recent AS prev_units,
        LEAD(units) OVER w_recent AS next_units,
        ROW_NUMBER() OVER w_recent AS seq_no
    FROM store_sales
    WHERE revenue <= 2800
    WINDOW w_recent AS (PARTITION BY product_id ORDER BY sale_date, id)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        product_id,
        revenue,
        next_units,
        seq_no,
        ROW_NUMBER() OVER w_recent AS seq_no_2,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY revenue DESC, id) AS row_seq
    FROM enriched
    WINDOW w_recent AS (PARTITION BY product_id ORDER BY prev_units DESC, id)
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(next_units / SUM(next_units) OVER (PARTITION BY product_id), 4) AS next_units_share,
        FIRST_VALUE(seq_no) IGNORE NULLS OVER (ORDER BY seq_no_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_seq_no
    FROM with_windows
),
kept_keys AS (
    -- Set difference on keys
    SELECT product_id, id
    FROM scored
    EXCEPT
    SELECT product_id, id
    FROM store_sales
    WHERE sale_date >= '2024-01-01'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM scored s
    INNER JOIN kept_keys k ON s.product_id = k.product_id AND s.id = k.id
)
SELECT
    product_id,
    revenue,
    seq_no_2,
    row_seq,
    next_units_share,
    first_known_seq_no
FROM reconciled;
