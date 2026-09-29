WITH valid AS (
    -- Basic cleansing and defaults
    SELECT *, GREATEST(units, revenue) AS greatest_units_revenue
    FROM store_sales
    WHERE revenue > 3300
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        greatest_units_revenue,
        LEAD(units, 1) OVER (ORDER BY greatest_units_revenue, id) AS next_units,
        NTILE(5) OVER w_running AS greatest_units_revenue_bucket,
        NTILE(3) OVER w_running AS greatest_units_revenue_bucket_2,
        SUM(greatest_units_revenue) OVER (PARTITION BY product_id ORDER BY units, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_greatest_units_revenue
    FROM valid
    WINDOW w_running AS (PARTITION BY store_id, product_id ORDER BY greatest_units_revenue DESC, id)
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, RANK() OVER (PARTITION BY store_id ORDER BY revenue DESC NULLS FIRST) AS dedup_rank
    FROM enriched
    QUALIFY dedup_rank = 1
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        greatest_units_revenue,
        next_units,
        greatest_units_revenue_bucket,
        greatest_units_revenue_bucket_2,
        cumulative_greatest_units_revenue,
        dedup_rank,
        AVG(greatest_units_revenue_bucket) OVER (ORDER BY next_units, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS greatest_units_revenue_bucket_avg_2_prior,
        COUNT(cumulative_greatest_units_revenue) OVER (PARTITION BY product_id) AS product_id_count_cumulative_greatest_units_revenue
    FROM deduped
),
enriched_store_sales AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        greatest_units_revenue,
        next_units,
        greatest_units_revenue_bucket,
        greatest_units_revenue_bucket_2,
        cumulative_greatest_units_revenue,
        dedup_rank,
        greatest_units_revenue_bucket_avg_2_prior,
        product_id_count_cumulative_greatest_units_revenue,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY cumulative_greatest_units_revenue, id) AS seq_no,
        ROW_NUMBER() OVER (PARTITION BY product_id, store_id ORDER BY greatest_units_revenue_bucket, id) AS event_seq,
        AVG(revenue) OVER (ORDER BY dedup_rank, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS revenue_avg_5_prior
    FROM metrics
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_id,
        sale_date,
        revenue,
        greatest_units_revenue,
        greatest_units_revenue_bucket,
        greatest_units_revenue_bucket_2,
        greatest_units_revenue_bucket_avg_2_prior,
        product_id_count_cumulative_greatest_units_revenue,
        event_seq,
        revenue_avg_5_prior,
        seq_no * units AS seq_no_times_units
    FROM enriched_store_sales
    WHERE sale_date > DATE '2023-10-07'
)
SELECT
    id,
    store_id,
    sale_date,
    revenue,
    greatest_units_revenue,
    greatest_units_revenue_bucket_2,
    product_id_count_cumulative_greatest_units_revenue,
    event_seq,
    revenue_avg_5_prior
FROM filtered
