WITH ranked AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        store_id,
        revenue
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY units DESC, id) AS rank_in_group
        FROM store_sales
    ) t
    WHERE rank_in_group = 1
),
enriched AS (
    -- Attach stores attributes
    SELECT
        s.id,
        s.store_id,
        p2.city
    FROM ranked s
    INNER JOIN stores p2
        ON s.store_id = p2.id
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        INITCAP(TRIM(city)) AS city_title,
        COALESCE(city, 'N/A') AS resolved_city
    FROM enriched
    WHERE city = 'Tokyo'
),
enriched_store_sales AS (
    -- Attach stores attributes
    SELECT
        e.*,
        s.opened_date,
        s.sqft,
        s.region
    FROM filtered e
    LEFT JOIN stores s
        ON e.store_id = s.id
    WHERE s.city IN ('Tokyo', 'Singapore')
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        city_title,
        resolved_city,
        sqft,
        region,
        LOWER(city) AS city_lower
    FROM enriched_store_sales
    WHERE city IS NOT DISTINCT FROM 'Bangkok'
      AND store_id IN (3, 6)
),
ranked_store_sales AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id, region ORDER BY sqft NULLS FIRST, id) AS dedup_rank
        FROM base
    ) t
    WHERE dedup_rank = 1
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        resolved_city,
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY sqft DESC, id) AS row_seq,
        SUM(sqft) OVER (ORDER BY dedup_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_sqft
    FROM ranked_store_sales
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN cumulative_sqft >= 77371 THEN 'LARGE' ELSE 'SMALL' END AS cumulative_sqft_tier,
    PERCENTILE_APPROX(row_seq, 0.5) AS row_seq_p50,
    COUNT(*) AS num_rows,
    ROUND(AVG(cumulative_sqft), 2) AS avg_cumulative_sqft,
    APPROX_PERCENTILE(row_seq, ARRAY(0.25, 0.5, 0.75)) AS row_seq_quartiles,
    APPROX_PERCENTILE(row_seq, 0.5) AS row_seq_p50_2,
    COUNT(*) AS cnt
FROM with_windows
GROUP BY cumulative_sqft_tier
ORDER BY avg_cumulative_sqft DESC NULLS FIRST
