WITH enriched AS (
    SELECT
        id,
        sqft,
        ROUND(sqft / SUM(sqft) OVER (PARTITION BY region), 4) AS sqft_share,
        FIRST_VALUE(region) IGNORE NULLS OVER (PARTITION BY region ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_region,
        SUM(sqft) OVER (PARTITION BY region) AS region_sum_sqft,
        ROW_NUMBER() OVER (PARTITION BY city, region ORDER BY sqft, id) AS event_seq,
        ROUND(sqft / SUM(sqft) OVER (PARTITION BY city), 4) AS sqft_share_2
    FROM stores
),
metrics AS (
    SELECT
        *,
        AVG(sqft_share) OVER w_ordered AS last7_avg_sqft_share,
        MIN(sqft_share) OVER w_ordered AS last7_min_sqft_share,
        MAX(sqft_share) OVER w_ordered AS last7_max_sqft_share
    FROM enriched
    WINDOW w_ordered AS (ORDER BY sqft, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
),
base AS (
    SELECT
        id,
        sqft,
        sqft_share,
        first_known_region,
        region_sum_sqft,
        event_seq,
        last7_avg_sqft_share,
        LEAST(sqft_share_2, region_sum_sqft) AS least_sqft_share_2_region_sum_sqft,
        GREATEST(last7_min_sqft_share, region_sum_sqft) AS greatest_last7_min_sqft_share_region_sum_sqft
    FROM metrics
    WHERE sqft_share_2 IS NOT NULL
),
cleaned AS (
    SELECT
        id,
        sqft,
        region_sum_sqft,
        event_seq,
        last7_avg_sqft_share,
        least_sqft_share_2_region_sum_sqft,
        greatest_last7_min_sqft_share_region_sum_sqft,
        ROUND(last7_avg_sqft_share, 1) AS last7_avg_sqft_share_rounded
    FROM base
    WHERE least_sqft_share_2_region_sum_sqft < 36
),
eligible AS (
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory c
            WHERE c.store_id = s.id
              AND c.store_id IN (4, 5, 6)
        )
),
latest_match AS (
    SELECT s.*, top1.store_sale_sale_date
    FROM eligible s
    JOIN LATERAL (
            SELECT c.sale_date AS store_sale_sale_date
            FROM store_sales c
            WHERE c.store_id = s.id
            ORDER BY c.sale_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
not_flagged AS (
    SELECT s.*
    FROM latest_match s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders c
            WHERE c.store_id = s.id
              AND c.order_date <= s.store_sale_sale_date
        )
)
SELECT
    id,
    sqft,
    region_sum_sqft,
    event_seq,
    last7_avg_sqft_share,
    least_sqft_share_2_region_sum_sqft,
    store_sale_sale_date
FROM not_flagged
ORDER BY sqft NULLS FIRST, region_sum_sqft;
