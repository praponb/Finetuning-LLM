WITH with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        RANK() OVER (PARTITION BY customer_id ORDER BY monthly_fee DESC) AS monthly_fee_rank,
        AVG(monthly_fee) OVER (PARTITION BY status ORDER BY monthly_fee, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS monthly_fee_avg_5,
        SUM(monthly_fee) OVER (PARTITION BY customer_id, status ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_monthly_fee,
        LAG(monthly_fee, 1) OVER (ORDER BY end_date, id) AS prev_monthly_fee
    FROM subscriptions
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY plan ORDER BY monthly_fee_avg_5 DESC, id) AS latest_rank
    FROM with_windows
    QUALIFY latest_rank = 1
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        status,
        monthly_fee_rank,
        monthly_fee_avg_5,
        prev_monthly_fee,
        latest_rank
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY monthly_fee_avg_5 DESC, id) AS dedup_rank
        FROM latest
    ) ranked
    WHERE dedup_rank = 1
),
valid AS (
    -- Basic cleansing and defaults
    SELECT *, latest_rank + prev_monthly_fee AS latest_rank_plus_prev_monthly_fee
    FROM deduped
    WHERE monthly_fee_rank < 5
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        status,
        monthly_fee_rank,
        monthly_fee_avg_5,
        latest_rank,
        DENSE_RANK() OVER (ORDER BY prev_monthly_fee NULLS LAST) AS prev_monthly_fee_dense_rank,
        AVG(prev_monthly_fee) OVER (PARTITION BY status ORDER BY latest_rank_plus_prev_monthly_fee, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS prev_monthly_fee_avg_2_prior,
        NTILE(4) OVER w_recent AS prev_monthly_fee_bucket,
        NTILE(5) OVER w_recent AS prev_monthly_fee_bucket_2
    FROM valid
    WINDOW w_recent AS (PARTITION BY status ORDER BY prev_monthly_fee DESC, id)
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        status,
        monthly_fee_rank,
        monthly_fee_avg_5,
        latest_rank,
        prev_monthly_fee_dense_rank,
        prev_monthly_fee_avg_2_prior,
        SUM(monthly_fee_rank) OVER (PARTITION BY status ORDER BY prev_monthly_fee_dense_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_monthly_fee_rank,
        COUNT(monthly_fee_rank) OVER (PARTITION BY status ORDER BY prev_monthly_fee_dense_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_monthly_fee_rank,
        MIN(monthly_fee_rank) OVER (PARTITION BY status ORDER BY prev_monthly_fee_dense_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_monthly_fee_rank
    FROM scored
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        status,
        running_sum_monthly_fee_rank,
        running_min_monthly_fee_rank
    FROM enriched
) src
UNPIVOT INCLUDE NULLS (
    value FOR measure IN (running_sum_monthly_fee_rank, running_min_monthly_fee_rank)
)
