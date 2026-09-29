WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LENGTH(city) city_length,
        INITCAP(TRIM(region)) region_title
    FROM stores
    WHERE opened_date > TO_DATE('2024-02-22')
      AND region IS NOT NULL
      AND city IN ('Singapore', 'Bangkok')
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        region_title,
        CASE WHEN city IN ('Tokyo', 'Berlin') THEN 'Priority' ELSE 'Other' END city_group
    FROM valid
    WHERE opened_date >= TO_DATE('2023-08-11')
),
with_latest AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.quantity,
        latest.last_restock
    FROM cleaned s
    JOIN LATERAL (
            SELECT c.quantity, c.last_restock
            FROM inventory c
            WHERE c.store_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(4) OVER (ORDER BY quantity, id) quantity_bucket,
        MIN(quantity) OVER (PARTITION BY city, region) city_min_quantity,
        NTILE(3) OVER (PARTITION BY city ORDER BY quantity DESC, id) quantity_bucket_2
    FROM with_latest
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM scored s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales c
            WHERE c.store_id = s.id
              AND c.sale_date BETWEEN '2023-12-10' AND '2024-01-01'
        )
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        region_title,
        city_group,
        quantity,
        last_restock,
        quantity_bucket,
        city_min_quantity,
        quantity_bucket_2,
        AVG(quantity_bucket_2) OVER (PARTITION BY region, city) region_avg_quantity_bucket_2,
        NTILE(4) OVER (PARTITION BY region ORDER BY quantity_bucket DESC, id) quantity_bucket_bucket
    FROM not_flagged
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('MONTH', opened_date) AS activity_month,
    DATE_TRUNC('YEAR', opened_date) AS year_start,
    SIZE(COLLECT_SET(region)) AS num_regions,
    SUM(quantity_bucket_2) AS total_quantity_bucket_2,
    PERCENTILE_APPROX(quantity, 0.95) AS quantity_p95,
    COUNT(*) AS row_count,
    COUNT(*) AS cnt,
    PERCENTILE_APPROX(quantity, ARRAY(0.25, 0.5, 0.75)) AS quantity_quartiles
FROM enriched
GROUP BY DATE_TRUNC('MONTH', opened_date), DATE_TRUNC('YEAR', opened_date);
