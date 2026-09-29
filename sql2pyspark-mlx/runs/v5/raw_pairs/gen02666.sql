WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, revenue * units AS revenue_per_unit
    FROM store_sales
    WHERE sale_date > DATE '2023-02-16'
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(revenue) OVER (PARTITION BY product_id ORDER BY revenue, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS revenue_avg_6_prior,
        NTILE(10) OVER (PARTITION BY product_id ORDER BY revenue, id) AS revenue_bucket
    FROM valid
),
enriched AS (
    -- Attach stores attributes
    SELECT
        src.*,
        d.sqft,
        d.store_name,
        d.city
    FROM scored src
    LEFT JOIN stores d
        ON src.store_id = d.id
),
scored_store_sales AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        product_id,
        sale_date,
        units,
        revenue,
        revenue_avg_6_prior,
        revenue_bucket,
        sqft,
        LAG(revenue_bucket, 1) OVER (PARTITION BY store_id ORDER BY units, id) AS prev_revenue_bucket,
        FIRST_VALUE(sqft, TRUE) OVER (PARTITION BY city ORDER BY sale_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_sqft,
        PERCENT_RANK() OVER (PARTITION BY city, store_id ORDER BY sqft NULLS LAST) AS sqft_percent_rank,
        ROW_NUMBER() OVER (ORDER BY units DESC, id) AS event_seq,
        LAST_VALUE(revenue_per_unit, TRUE) OVER (PARTITION BY city, product_id ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_revenue_per_unit
    FROM enriched
),
valid_store_sales AS (
    -- Keep recent, valid rows
    SELECT
        id,
        product_id,
        sale_date,
        units,
        revenue,
        revenue_avg_6_prior,
        revenue_bucket,
        sqft,
        prev_revenue_bucket,
        first_known_sqft,
        sqft_percent_rank,
        event_seq,
        last_known_revenue_per_unit,
        sqft / NULLIF(sqft_percent_rank, 0) AS sqft_per_sqft_percent_rank
    FROM scored_store_sales
    WHERE sale_date >= DATE '2023-07-17'
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        sale_date,
        units,
        revenue_avg_6_prior,
        prev_revenue_bucket,
        first_known_sqft,
        sqft_percent_rank,
        sqft_per_sqft_percent_rank,
        LEAD(prev_revenue_bucket, 2) OVER (PARTITION BY product_id ORDER BY revenue_avg_6_prior, id) AS next_prev_revenue_bucket,
        AVG(last_known_revenue_per_unit) OVER (PARTITION BY product_id ORDER BY prev_revenue_bucket, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS last_known_revenue_per_unit_avg_5,
        AVG(revenue) OVER (PARTITION BY product_id ORDER BY sqft_percent_rank, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS revenue_avg_5_prior,
        SUM(sqft) OVER (PARTITION BY product_id ORDER BY last_known_revenue_per_unit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_sqft
    FROM valid_store_sales
),
enriched_store_sales AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        sale_date,
        units,
        prev_revenue_bucket,
        first_known_sqft,
        next_prev_revenue_bucket,
        revenue_avg_5_prior,
        cumulative_sqft,
        AVG(sqft_per_sqft_percent_rank) OVER (ORDER BY prev_revenue_bucket, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS sqft_per_sqft_percent_rank_avg_5_prior,
        ROW_NUMBER() OVER (ORDER BY revenue_avg_6_prior, id) AS seq_no
    FROM with_windows
    WHERE next_prev_revenue_bucket IS NOT NULL
)
SELECT
    id,
    sale_date,
    units,
    prev_revenue_bucket,
    cumulative_sqft
FROM enriched_store_sales
