WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_name,
        city,
        opened_date,
        sqft,
        LENGTH(city) AS city_length,
        COALESCE(region, 'NONE') AS region_clean
    FROM stores
    WHERE opened_date >= TO_DATE('2023-06-20')
      AND region IS NOT NULL
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_name,
        city,
        sqft,
        city_length,
        region_clean,
        COUNT(city_length) OVER w_ordered AS last4_count_city_length,
        MIN(city_length) OVER w_ordered AS last4_min_city_length,
        MAX(city_length) OVER w_ordered AS last4_max_city_length,
        SUM(sqft) OVER (PARTITION BY city ORDER BY sqft ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_sqft
    FROM cleaned
    WHERE city_length <= 36
    WINDOW w_ordered AS (PARTITION BY city ORDER BY opened_date ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),
cleaned_stores AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ROUND(cumulative_sqft / last4_count_city_length, 2) AS cumulative_sqft_per_last4_count_city_length
    FROM scored
    WHERE last4_max_city_length > 62
),
with_last AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.store_sale_units,
        latest.store_sale_revenue
    FROM cleaned_stores s
    JOIN LATERAL (
            SELECT c.units AS store_sale_units, c.revenue AS store_sale_revenue
            FROM store_sales c
            WHERE c.store_id = s.id
            ORDER BY c.sale_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(4) OVER (PARTITION BY city ORDER BY last4_count_city_length DESC, id ASC) AS last4_count_city_length_bucket,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY sqft DESC, id ASC) AS event_seq,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY last4_max_city_length DESC, id ASC) AS seq_no,
        LEAD(store_sale_units, 1) OVER (PARTITION BY city ORDER BY last4_min_city_length ASC, id ASC) AS next_store_sale_units,
        CUME_DIST() OVER (PARTITION BY city ORDER BY last4_max_city_length ASC) AS last4_max_city_length_cume_dist
    FROM with_last
)
SELECT
    id,
    store_name,
    city,
    sqft,
    city_length,
    region_clean,
    last4_max_city_length,
    cumulative_sqft,
    cumulative_sqft_per_last4_count_city_length,
    CASE WHEN city RLIKE '^(London|New York)$' THEN 'GROUP_A' WHEN city RLIKE '^(Bangkok|Chiang Mai)$' THEN 'CORE' ELSE 'GROUP_B' END AS city_zone
FROM with_windows
ORDER BY last4_max_city_length DESC NULLS LAST, id ASC
