WITH with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER w_running AS seq_no,
        FIRST_VALUE(units, TRUE) OVER (ORDER BY sale_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_units,
        ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY units DESC, id) AS row_seq
    FROM store_sales
    WINDOW w_running AS (PARTITION BY product_id ORDER BY sale_date DESC, id)
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, revenue * seq_no AS revenue_times_seq_no
    FROM with_windows
    WHERE sale_date >= DATE '2023-11-29'
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        revenue,
        seq_no,
        first_known_units,
        revenue_times_seq_no,
        seq_no - revenue_times_seq_no AS seq_no_minus_revenue_times_seq_no
    FROM filtered
    WHERE sale_date >= DATE '2024-01-11'
)
SELECT
    id,
    store_id,
    product_id,
    sale_date,
    revenue,
    first_known_units
FROM base
ORDER BY sale_date;
