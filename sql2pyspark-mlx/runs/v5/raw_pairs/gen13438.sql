WITH latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY opened_date DESC, id) AS dedup_rank
    FROM stores
    QUALIFY dedup_rank = 1
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(5) OVER (PARTITION BY city ORDER BY sqft DESC, id) AS sqft_bucket,
        DENSE_RANK() OVER (PARTITION BY region ORDER BY sqft DESC NULLS FIRST) AS sqft_dense_rank,
        NTILE(4) OVER (PARTITION BY region ORDER BY sqft, id) AS sqft_bucket_2
    FROM latest
),
with_latest AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.store_name,
        s.region,
        s.opened_date,
        s.sqft_bucket,
        s.sqft_dense_rank,
        x.quantity
    FROM metrics s
    LEFT JOIN LATERAL (
            SELECT c.quantity
            FROM inventory c
            WHERE c.store_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
with_latest_stores AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.opened_date,
        s.sqft_dense_rank,
        top1.store_sale_units
    FROM with_latest s
    LEFT JOIN LATERAL (
            SELECT c.units AS store_sale_units
            FROM store_sales c
            WHERE c.store_id = s.id
            ORDER BY c.units DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM with_latest_stores s
    WHERE EXISTS (
            SELECT 1
            FROM store_sales ch
            WHERE ch.store_id = s.id
              AND ch.sale_date <= s.opened_date
        )
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, YEAR(opened_date) AS opened_date_year
    FROM eligible
    WHERE store_sale_units > 38
)
-- Grouped metrics
SELECT
    CASE WHEN opened_date_year >= 64 THEN 'LARGE' ELSE 'SMALL' END AS opened_date_year_tier,
    ROUND(AVG(opened_date_year), 2) AS avg_opened_date_year,
    COUNT(*) AS num_rows,
    PERCENTILE_APPROX(opened_date_year, 0.9) AS opened_date_year_p90,
    PERCENTILE_APPROX(sqft_dense_rank, ARRAY(0.25, 0.5, 0.75)) AS sqft_dense_rank_quartiles,
    MIN(opened_date_year) AS min_opened_date_year
FROM cleaned
GROUP BY CASE WHEN opened_date_year >= 64 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY avg_opened_date_year
