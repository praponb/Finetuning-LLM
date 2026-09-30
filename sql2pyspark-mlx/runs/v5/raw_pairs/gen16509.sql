WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        COALESCE(sqft, 0) AS sqft_filled
    FROM stores
    WHERE opened_date >= DATE '2023-08-21'
      AND region IS NOT NULL
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM cleaned s
    WHERE EXISTS (
            SELECT 1
            FROM inventory x
            WHERE x.store_id = s.id
              AND x.last_restock >= s.opened_date
        )
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        city,
        region,
        sqft,
        sqft_filled,
        sqft_filled - sqft AS sqft_filled_minus_sqft,
        DATE_TRUNC('year', opened_date) AS opened_date_period,
        COALESCE(region, 'N/A') AS resolved_region
    FROM not_flagged
    WHERE region IS NOT NULL
),
base_stores AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN sqft_filled >= 70 THEN 'High' WHEN sqft_filled >= 33 THEN 'Medium' ELSE 'Low' END AS sqft_filled_band
    FROM base
    WHERE sqft_filled BETWEEN 31 AND 45
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY region ORDER BY opened_date_period, id) AS dedup_rank
        FROM base_stores
    ) x
    WHERE dedup_rank = 1
),
with_latest AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.region,
        s.sqft_filled,
        s.sqft_filled_minus_sqft,
        s.sqft_filled_band,
        s.dedup_rank,
        top1.order_date,
        top1.discount
    FROM deduped s
    LEFT JOIN LATERAL (
            SELECT c.order_date, c.discount
            FROM orders c
            WHERE c.store_id = s.id
            ORDER BY c.shipping_fee DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        sqft_filled_band,
        discount,
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY sqft_filled, id) AS row_seq,
        LAST_VALUE(sqft_filled_minus_sqft, TRUE) OVER (PARTITION BY region ORDER BY sqft_filled_minus_sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_sqft_filled_minus_sqft
    FROM with_latest
    WHERE region = 'South'
)
-- Grouped metrics
SELECT
    CASE WHEN last_known_sqft_filled_minus_sqft >= 67 THEN 'PREMIUM' ELSE 'STANDARD' END AS last_known_sqft_filled_minus_sqft_tier,
    COUNT(DISTINCT id) AS distinct_ids,
    MIN(last_known_sqft_filled_minus_sqft) AS min_last_known_sqft_filled_minus_sqft,
    APPROX_PERCENTILE(row_seq, ARRAY(0.25, 0.5, 0.75)) AS row_seq_quartiles,
    COUNT(DISTINCT id) AS distinct_ids_2
FROM metrics
GROUP BY CASE WHEN last_known_sqft_filled_minus_sqft >= 67 THEN 'PREMIUM' ELSE 'STANDARD' END;
