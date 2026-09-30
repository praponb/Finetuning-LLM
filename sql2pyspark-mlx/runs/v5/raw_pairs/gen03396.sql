WITH cleaned AS (
    SELECT
        id,
        city,
        region,
        opened_date,
        sqft,
        DATEDIFF(CURRENT_DATE(), opened_date) AS days_since_opened_date
    FROM stores
    WHERE region IS NOT DISTINCT FROM 'South'
),
metrics AS (
    SELECT
        id,
        city,
        region,
        opened_date,
        days_since_opened_date,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY days_since_opened_date, id) AS event_seq,
        AVG(sqft) OVER (PARTITION BY region, city ORDER BY days_since_opened_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS sqft_avg_6
    FROM cleaned
    WHERE days_since_opened_date <> 34
),
with_latest AS (
    SELECT
        s.*,
        top1.discount,
        top1.status
    FROM metrics s
    LEFT JOIN LATERAL (
            SELECT c.discount, c.status
            FROM orders c
            WHERE c.store_id = s.id
            ORDER BY c.order_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_windows AS (
    SELECT
        *,
        FIRST_VALUE(status) IGNORE NULLS OVER w_part AS first_known_status,
        LEAD(days_since_opened_date, 1) OVER (PARTITION BY region ORDER BY days_since_opened_date, id) AS next_days_since_opened_date,
        LEAD(days_since_opened_date, 1) OVER (PARTITION BY city ORDER BY discount, id) AS next_days_since_opened_date_2,
        NTILE(3) OVER (PARTITION BY status, city ORDER BY sqft_avg_6 DESC, id) AS sqft_avg_6_bucket
    FROM with_latest
    WINDOW w_part AS (PARTITION BY status, region ORDER BY discount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
totals AS (
    SELECT
        CASE WHEN next_days_since_opened_date >= 87 THEN 'HIGH' ELSE 'LOW' END AS next_days_since_opened_date_tier,
        CASE WHEN days_since_opened_date >= 66 THEN 'LARGE' ELSE 'SMALL' END AS days_since_opened_date_tier,
        COUNT(*) FILTER (WHERE status = 'PENDING') AS pending_rows,
        APPROX_PERCENTILE(next_days_since_opened_date, ARRAY(0.25, 0.5, 0.75)) AS next_days_since_opened_date_quartiles
    FROM with_windows
    GROUP BY ROLLUP (CASE WHEN next_days_since_opened_date >= 87 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN days_since_opened_date >= 66 THEN 'LARGE' ELSE 'SMALL' END)
)
SELECT
    next_days_since_opened_date_tier,
    days_since_opened_date_tier,
    pending_rows,
    next_days_since_opened_date_quartiles
FROM totals
ORDER BY days_since_opened_date_tier DESC
