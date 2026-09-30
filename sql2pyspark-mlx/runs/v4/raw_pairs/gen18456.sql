WITH cleaned AS (
    SELECT
        id,
        store_name,
        opened_date,
        sqft,
        INITCAP(TRIM(store_name)) AS store_name_title,
        COALESCE(region, 'N/A') AS resolved_region
    FROM stores
    WHERE region = 'North'
)
SELECT
    *,
    AVG(sqft) OVER (ORDER BY opened_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS sqft_avg_2,
    NTILE(4) OVER w_ordered AS sqft_bucket,
    SUM(sqft) OVER (ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_sqft,
    NTILE(4) OVER w_ordered AS sqft_bucket_2
FROM cleaned
WINDOW w_ordered AS (ORDER BY sqft DESC, id)
