WITH valid AS (
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        ROUND(sqft * 1.15, 2) AS sqft_scaled,
        CONCAT_WS(',', store_name, city) AS store_name_city_key,
        COALESCE(region, 'DIRECT') AS resolved_region
    FROM stores
    WHERE opened_date > DATE '2023-05-02'
)
SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY region, city ORDER BY sqft_scaled DESC, id) AS row_seq,
    FIRST_VALUE(sqft, TRUE) OVER (PARTITION BY city, region ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_sqft,
    LAST_VALUE(region, TRUE) OVER (ORDER BY sqft_scaled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_region,
    AVG(sqft_scaled) OVER w_recent AS sqft_scaled_avg_6
FROM valid
WINDOW w_recent AS (PARTITION BY city ORDER BY sqft_scaled, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW);
