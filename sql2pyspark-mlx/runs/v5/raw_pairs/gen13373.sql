WITH not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM stores s
    WHERE EXISTS (
            SELECT 1
            FROM inventory ch
            WHERE ch.store_id = s.id
              AND ch.product_id = 9
        )
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY sqft, id) AS rank_in_group
        FROM not_flagged
    ) x
    WHERE rank_in_group = 1
),
with_latest AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.store_sale_units,
        x.store_sale_sale_date
    FROM deduped s
    JOIN LATERAL (
            SELECT c.units AS store_sale_units, c.sale_date AS store_sale_sale_date
            FROM store_sales c
            WHERE c.store_id = s.id
            ORDER BY c.revenue DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(store_sale_units, 1) OVER (ORDER BY sqft, id) prev_store_sale_units,
        ROW_NUMBER() OVER (ORDER BY sqft DESC, id) row_seq,
        LAST_VALUE(region, TRUE) OVER (PARTITION BY city ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_region
    FROM with_latest
    WHERE MONTH(store_sale_sale_date) IN (3, 8, 12)
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        city,
        opened_date,
        sqft,
        store_sale_sale_date,
        last_known_region
    FROM enriched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY region ORDER BY opened_date DESC, id) = 1
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('QUARTER', opened_date) AS quarter_start,
    CASE WHEN sqft >= 14576 THEN 'PREMIUM' ELSE 'STANDARD' END AS sqft_tier,
    COUNT(*) AS row_count,
    APPROX_PERCENTILE(sqft, ARRAY(0.25, 0.5, 0.75)) AS sqft_quartiles,
    ROUND(AVG(sqft), 2) AS avg_sqft,
    ROUND(AVG(sqft), 2) AS avg_sqft_2,
    SUM(sqft) AS total_sqft,
    COUNT_IF(city = 'London') AS london_events
FROM latest
GROUP BY ROLLUP (DATE_TRUNC('QUARTER', opened_date), CASE WHEN sqft >= 14576 THEN 'PREMIUM' ELSE 'STANDARD' END)
ORDER BY avg_sqft DESC;
