WITH valid AS (
    SELECT
        *,
        FLOOR(sqft) AS sqft_whole,
        LAST_DAY(opened_date) AS opened_date_month_end
    FROM stores
    WHERE opened_date >= DATE '2023-09-30'
      AND region IS NULL
)
SELECT
    id,
    store_name,
    opened_date,
    RANK() OVER w_grp AS sqft_rank,
    SUM(sqft_whole) OVER (PARTITION BY region, city ORDER BY opened_date_month_end, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_sqft_whole,
    LAST_VALUE(sqft, TRUE) OVER (ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_sqft,
    LAST_VALUE(region) IGNORE NULLS OVER (PARTITION BY city ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_region
FROM valid
WHERE region IS NULL
WINDOW w_grp AS (PARTITION BY region ORDER BY sqft DESC);
