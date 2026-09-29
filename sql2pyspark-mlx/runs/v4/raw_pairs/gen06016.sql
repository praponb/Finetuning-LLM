WITH valid AS (
    SELECT
        *,
        DATE_SUB(opened_date, 35) AS opened_date_minus_35d,
        COALESCE(region, 'DIRECT') AS resolved_region
    FROM stores
    WHERE sqft <> 9520
),
valid_stores AS (
    SELECT
        id,
        city,
        region,
        opened_date,
        sqft,
        opened_date_minus_35d,
        resolved_region,
        LENGTH(city) AS city_length,
        COALESCE(region, 'UNKNOWN') AS resolved_region_2
    FROM valid
    WHERE opened_date >= DATE '2023-03-14'
      AND region IS NOT NULL
      AND store_name LIKE 'R%'
),
latest_match AS (
    SELECT s.*, latest.shipping_fee
    FROM valid_stores s
    LEFT JOIN LATERAL (
            SELECT c.shipping_fee
            FROM orders c
            WHERE c.store_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
cleaned AS (
    SELECT
        id,
        city,
        opened_date,
        sqft,
        opened_date_minus_35d,
        resolved_region,
        resolved_region_2,
        shipping_fee,
        sqft * shipping_fee AS sqft_times_shipping_fee
    FROM latest_match
    WHERE opened_date > DATE '2023-07-05'
)
SELECT
    DATE_TRUNC('WEEK', opened_date) AS week_start,
    CASE WHEN sqft >= 16059 THEN 'LARGE' ELSE 'SMALL' END AS sqft_tier,
    city,
    COUNT(CASE WHEN city = 'Tokyo' THEN 1 END) AS tokyo_count,
    ROUND(SUM(sqft_times_shipping_fee) / NULLIF(COUNT(CASE WHEN city = 'Chiang Mai' THEN 1 END), 0), 2) AS sqft_times_shipping_fee_per_chiang_mai,
    COUNT(*) AS num_rows,
    DENSE_RANK() OVER (PARTITION BY DATE_TRUNC('WEEK', opened_date) ORDER BY COUNT(CASE WHEN city = 'Tokyo' THEN 1 END) DESC) AS tokyo_count_dense_rank
FROM cleaned
GROUP BY week_start, sqft_tier, city
HAVING COUNT(CASE WHEN city = 'Tokyo' THEN 1 END) > 1
