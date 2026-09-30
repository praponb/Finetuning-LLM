WITH enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        txn_time,
        merchant,
        FIRST_VALUE(merchant) IGNORE NULLS OVER (ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_merchant,
        NTILE(5) OVER (ORDER BY amount DESC, id) AS amount_bucket,
        CUME_DIST() OVER (ORDER BY amount DESC NULLS FIRST) AS amount_cume_dist,
        ROUND(amount / SUM(amount) OVER (PARTITION BY txn_type), 4) AS amount_share
    FROM transactions
    WHERE amount > 2300
),
enriched_transactions AS (
    -- Running and moving aggregates
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY merchant ORDER BY amount_share DESC) AS amount_share_percent_rank,
        LAST_VALUE(amount_cume_dist, TRUE) OVER (PARTITION BY merchant ORDER BY amount_cume_dist, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_amount_cume_dist
    FROM enriched
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROUND(amount_bucket / SUM(amount_bucket) OVER (PARTITION BY merchant), 4) AS amount_bucket_share,
        SUM(amount_share_percent_rank) OVER (PARTITION BY merchant ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS amount_share_percent_rank_sum_30d,
        ROW_NUMBER() OVER (PARTITION BY merchant ORDER BY amount_share_percent_rank DESC, id) AS seq_no,
        MAX(amount_bucket) OVER (PARTITION BY merchant) AS merchant_max_amount_bucket
    FROM enriched_transactions
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        txn_time,
        first_known_merchant,
        amount_bucket,
        amount_cume_dist,
        amount_share,
        amount_share_percent_rank,
        last_known_amount_cume_dist,
        seq_no
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY merchant ORDER BY amount_bucket_share DESC NULLS LAST, id) AS dedup_rank
        FROM scored
    ) ranked
    WHERE dedup_rank = 1
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(amount_cume_dist, 1) OVER (ORDER BY txn_time, id) AS next_amount_cume_dist,
        NTILE(3) OVER (ORDER BY amount_bucket, id) AS amount_bucket_bucket,
        FIRST_VALUE(amount_share_percent_rank, TRUE) OVER (ORDER BY amount_share_percent_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_amount_share_percent_rank,
        AVG(seq_no) OVER (ORDER BY amount_bucket, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS seq_no_avg_2_prior
    FROM ranked
)
-- Window metrics over several different partitions
SELECT
    id,
    first_known_merchant,
    amount_bucket,
    amount_share_percent_rank,
    last_known_amount_cume_dist,
    next_amount_cume_dist,
    first_known_amount_share_percent_rank,
    seq_no_avg_2_prior,
    LAST_VALUE(amount_share, TRUE) OVER (ORDER BY amount_share, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_amount_share,
    LAST_VALUE(first_known_amount_share_percent_rank) IGNORE NULLS OVER (ORDER BY amount_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_first_known_amount_share_percent_rank,
    AVG(seq_no_avg_2_prior) OVER (ORDER BY seq_no, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS seq_no_avg_2_prior_avg_5_prior
FROM with_windows
