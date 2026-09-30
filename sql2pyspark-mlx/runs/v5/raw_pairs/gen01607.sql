WITH valid AS (
    SELECT
        *,
        CONCAT_WS('-', city, store_name) AS city_store_name_key,
        CASE WHEN city IN ('Tokyo', 'Chiang Mai') THEN 'Core' ELSE 'Other' END AS city_group,
        COALESCE(region, 'DIRECT') AS region_clean
    FROM stores
    WHERE opened_date > DATE '2023-06-27'
      AND region IS NOT NULL
),
latest_match AS (
    SELECT
        s.id,
        s.opened_date,
        s.sqft,
        s.city_store_name_key,
        x.store_sale_revenue
    FROM valid s
    LEFT JOIN LATERAL (
            SELECT c.revenue AS store_sale_revenue
            FROM store_sales c
            WHERE c.store_id = s.id
            ORDER BY c.units DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
scored AS (
    SELECT
        *,
        LAG(sqft) OVER (ORDER BY store_sale_revenue, id) AS prev_sqft,
        LEAD(sqft) OVER (ORDER BY store_sale_revenue, id) AS next_sqft,
        LEAD(store_sale_revenue) OVER (ORDER BY store_sale_revenue, id) AS next_store_sale_revenue,
        LAG(store_sale_revenue) OVER (ORDER BY store_sale_revenue, id) AS prev_store_sale_revenue,
        ROW_NUMBER() OVER (ORDER BY store_sale_revenue, id) AS row_seq
    FROM latest_match
    WHERE store_sale_revenue <= 1200
),
with_windows AS (
    SELECT
        id,
        opened_date,
        prev_sqft,
        row_seq,
        PERCENT_RANK() OVER (ORDER BY next_sqft) AS next_sqft_percent_rank,
        ROW_NUMBER() OVER (ORDER BY opened_date DESC, id) AS row_seq_2,
        SUM(next_store_sale_revenue) OVER (ORDER BY next_store_sale_revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_next_store_sale_revenue
    FROM scored
),
with_windows_stores AS (
    SELECT
        id,
        prev_sqft,
        next_sqft_percent_rank,
        row_seq_2,
        LEAD(row_seq_2) OVER w_grp AS next_row_seq_2,
        LEAD(cumulative_next_store_sale_revenue) OVER w_grp AS next_cumulative_next_store_sale_revenue,
        ROW_NUMBER() OVER w_grp AS row_seq_3,
        NTILE(5) OVER (ORDER BY row_seq DESC, id) AS row_seq_bucket
    FROM with_windows
    WINDOW w_grp AS (ORDER BY row_seq_2, id)
),
latest_match_stores AS (
    SELECT
        s.*,
        top1.store_sale_sale_date,
        top1.units
    FROM with_windows_stores s
    JOIN LATERAL (
            SELECT c.sale_date AS store_sale_sale_date, c.units
            FROM store_sales c
            WHERE c.store_id = s.id
            ORDER BY c.sale_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
SELECT
    CASE WHEN row_seq_bucket >= 8 THEN 'HIGH' ELSE 'LOW' END AS row_seq_bucket_tier,
    COUNT(*) AS num_rows,
    SUM(next_sqft_percent_rank) AS total_next_sqft_percent_rank,
    APPROX_PERCENTILE(units, 0.5) AS units_p50
FROM latest_match_stores
GROUP BY row_seq_bucket_tier
ORDER BY row_seq_bucket_tier DESC;
