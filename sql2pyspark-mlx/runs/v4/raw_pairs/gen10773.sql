WITH with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        DENSE_RANK() OVER (PARTITION BY city ORDER BY sqft DESC NULLS FIRST) AS sqft_dense_rank,
        SUM(sqft) OVER (PARTITION BY region ORDER BY opened_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sqft,
        DENSE_RANK() OVER (ORDER BY sqft DESC) AS sqft_dense_rank_2
    FROM stores
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        RANK() OVER (ORDER BY sqft_dense_rank_2 DESC) AS sqft_dense_rank_2_rank,
        FIRST_VALUE(sqft_dense_rank, TRUE) OVER (PARTITION BY region, city ORDER BY running_sqft ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_sqft_dense_rank
    FROM with_windows
),
scored_stores AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY city, region ORDER BY sqft ASC, id ASC) AS event_seq,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY sqft_dense_rank_2_rank ASC, id ASC) AS row_seq,
        PERCENT_RANK() OVER (PARTITION BY city ORDER BY sqft_dense_rank_2 DESC) AS sqft_dense_rank_2_percent_rank,
        ROUND(running_sqft / SUM(running_sqft) OVER (PARTITION BY city), 4) AS running_sqft_share
    FROM scored
)
-- Window metrics over several different partitions
SELECT
    id,
    store_name,
    city,
    region,
    opened_date,
    sqft,
    sqft_dense_rank,
    running_sqft,
    sqft_dense_rank_2,
    sqft_dense_rank_2_rank,
    first_known_sqft_dense_rank,
    event_seq,
    row_seq,
    sqft_dense_rank_2_percent_rank,
    running_sqft_share,
    AVG(first_known_sqft_dense_rank) OVER (PARTITION BY region, city ORDER BY event_seq ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS first_known_sqft_dense_rank_avg_6,
    SUM(running_sqft_share) OVER (PARTITION BY region ORDER BY event_seq ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_running_sqft_share,
    SUM(sqft_dense_rank_2_rank) OVER (PARTITION BY region ORDER BY event_seq ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sqft_dense_rank_2_rank
FROM scored_stores
