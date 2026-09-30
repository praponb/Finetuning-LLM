WITH scored AS (
    -- Ranking and gap features
    SELECT
        id,
        store_name,
        city,
        opened_date,
        SUM(sqft) OVER (PARTITION BY region ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sqft,
        CUME_DIST() OVER (PARTITION BY city, region ORDER BY sqft DESC NULLS LAST) AS sqft_cume_dist,
        LAST_VALUE(region, TRUE) OVER (ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_region
    FROM stores
)
SELECT
    id,
    store_name,
    opened_date,
    running_sqft,
    CASE WHEN store_name RLIKE '^(Mall Central|Old Town)$' THEN 'OTHER_ZONE' WHEN store_name RLIKE '^(Siam Flagship|Downtown Express)$' THEN 'PRIORITY' ELSE 'GROUP_B' END AS store_name_zone
FROM scored
