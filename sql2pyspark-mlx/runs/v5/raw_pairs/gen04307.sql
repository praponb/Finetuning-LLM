WITH filtered AS (
    SELECT
        id,
        store_name,
        city,
        region,
        sqft,
        CAST(sqft AS STRING) AS sqft_num,
        LOWER(city) AS city_lower
    FROM stores
    WHERE opened_date >= TO_DATE('2023-07-09')
      AND region IS NOT NULL
),
with_latest AS (
    SELECT s.*, top1.order_status
    FROM filtered s
    LEFT JOIN LATERAL (
            SELECT c.status AS order_status
            FROM orders c
            WHERE c.store_id = s.id
            ORDER BY c.order_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
scored AS (
    SELECT
        *,
        NTILE(5) OVER (PARTITION BY city ORDER BY sqft, id) AS sqft_bucket,
        MIN(sqft) OVER (PARTITION BY region) AS region_min_sqft
    FROM with_latest
),
metrics AS (
    SELECT
        *,
        NTILE(10) OVER w_recent AS region_min_sqft_bucket,
        ROW_NUMBER() OVER (PARTITION BY order_status ORDER BY sqft_bucket DESC, id) AS row_seq,
        SUM(region_min_sqft) OVER w_ordered AS running_region_min_sqft,
        LAG(region_min_sqft, 1) OVER (PARTITION BY city ORDER BY sqft_bucket, id) AS prev_region_min_sqft,
        NTILE(4) OVER w_part AS sqft_bucket_bucket
    FROM scored
    WINDOW w_recent AS (PARTITION BY order_status ORDER BY region_min_sqft, id), w_ordered AS (ORDER BY region_min_sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_part AS (PARTITION BY region ORDER BY sqft_bucket DESC, id)
)
SELECT
    id,
    store_name,
    region,
    sqft_num,
    city_lower,
    order_status,
    region_min_sqft,
    region_min_sqft_bucket,
    LEAD(running_region_min_sqft) OVER w_part AS next_running_region_min_sqft,
    LEAD(sqft) OVER w_part AS next_sqft,
    LAG(sqft) OVER w_part AS prev_sqft,
    ROW_NUMBER() OVER w_part AS row_seq_2
FROM metrics
WINDOW w_part AS (PARTITION BY region ORDER BY running_region_min_sqft, id)
