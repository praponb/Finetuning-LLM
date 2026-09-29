WITH enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY region, city ORDER BY sqft DESC, id ASC) AS row_seq,
        ROUND(sqft / SUM(sqft) OVER (PARTITION BY city, region), 4) AS sqft_share,
        SUM(sqft) OVER (ORDER BY opened_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_sqft,
        ROW_NUMBER() OVER w_recent AS row_seq_2,
        NTILE(4) OVER (PARTITION BY region, city ORDER BY sqft ASC, id ASC) AS sqft_bucket
    FROM stores
    WINDOW w_recent AS (PARTITION BY region ORDER BY sqft ASC, id ASC)
)
SELECT
    id,
    city,
    region,
    sqft,
    row_seq,
    cumulative_sqft,
    row_seq_2
FROM enriched
ORDER BY city ASC NULLS LAST, id ASC;
