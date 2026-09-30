WITH valid AS (
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        UPPER(TRIM(store_name)) AS store_name_normalized
    FROM stores
    WHERE opened_date > TO_DATE('2023-06-25')
),
with_latest AS (
    SELECT s.*, latest.store_sale_sale_date
    FROM valid s
    JOIN LATERAL (
            SELECT c.sale_date AS store_sale_sale_date
            FROM store_sales c
            WHERE c.store_id = s.id
            ORDER BY c.revenue DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
scored AS (
    SELECT
        id,
        store_name,
        city,
        region,
        store_name_normalized,
        store_sale_sale_date,
        LEAD(sqft) OVER w_running AS next_sqft,
        ROW_NUMBER() OVER w_running AS row_seq
    FROM with_latest
    WINDOW w_running AS (PARTITION BY region ORDER BY store_sale_sale_date, id)
),
eligible AS (
    SELECT s.*
    FROM scored s
    WHERE EXISTS (
            SELECT 1
            FROM inventory c
            WHERE c.store_id = s.id
              AND c.quantity < 162
        )
),
stats AS (
    SELECT
        CASE WHEN next_sqft >= 12866 THEN 'PREMIUM' ELSE 'STANDARD' END AS next_sqft_tier,
        DATE_TRUNC('QUARTER', store_sale_sale_date) AS quarter_start,
        SIZE(COLLECT_SET(region)) AS num_regions,
        ROUND(SUM(next_sqft) / NULLIF(COUNT(CASE WHEN city = 'Singapore' THEN 1 END), 0), 2) AS next_sqft_per_singapore,
        COUNT_IF(region = 'Central') AS central_events
    FROM eligible
    GROUP BY 1, 2
)
SELECT
    next_sqft_tier,
    quarter_start,
    central_events
FROM stats
ORDER BY next_sqft_tier DESC NULLS FIRST;
