WITH with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(5) OVER (ORDER BY sqft, id) AS sqft_bucket,
        NTILE(10) OVER (PARTITION BY region ORDER BY sqft DESC, id) AS sqft_bucket_2
    FROM stores
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY sqft_bucket_2 DESC NULLS FIRST, id) AS rank_in_group
        FROM with_windows
    ) ranked
    WHERE rank_in_group = 1
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM deduped
    QUALIFY ROW_NUMBER() OVER (PARTITION BY region ORDER BY sqft DESC NULLS LAST, id) = 1
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER w_recent AS row_seq,
        DENSE_RANK() OVER (PARTITION BY region, city ORDER BY sqft DESC NULLS FIRST) AS sqft_dense_rank,
        AVG(sqft_bucket_2) OVER (PARTITION BY city ORDER BY sqft, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS sqft_bucket_2_avg_5,
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY sqft DESC, id) AS event_seq,
        ROUND(sqft_bucket_2 / SUM(sqft_bucket_2) OVER (PARTITION BY region, city), 4) AS sqft_bucket_2_share
    FROM latest
    WINDOW w_recent AS (PARTITION BY region, city ORDER BY sqft, id)
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM metrics s
    WHERE EXISTS (
            SELECT 1
            FROM store_sales ch
            WHERE ch.store_id = s.id
              AND ch.sale_date <= s.opened_date
        )
),
with_latest AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.channel
    FROM kept s
    LEFT JOIN LATERAL (
            SELECT c.channel
            FROM orders c
            WHERE c.store_id = s.id
            ORDER BY c.discount DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
-- Ranking and gap features
SELECT
    *,
    AVG(sqft) OVER (PARTITION BY region ORDER BY opened_date, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS sqft_avg_2_prior,
    SUM(sqft) OVER (PARTITION BY city ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_sqft,
    ROUND(sqft_bucket_2_avg_5 / SUM(sqft_bucket_2_avg_5) OVER (PARTITION BY city), 4) AS sqft_bucket_2_avg_5_share,
    FIRST_VALUE(sqft, TRUE) OVER (PARTITION BY region ORDER BY sqft_bucket_2_avg_5, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_sqft
FROM with_latest
