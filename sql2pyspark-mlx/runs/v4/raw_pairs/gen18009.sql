WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        amount,
        merchant,
        is_flagged,
        CASE WHEN is_flagged THEN 'Yes' ELSE 'No' END AS is_flagged_label,
        COALESCE(merchant, 'UNKNOWN') AS merchant_clean
    FROM transactions
    WHERE txn_time > TO_DATE('2024-04-10')
      AND merchant IS NOT NULL
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM valid
    QUALIFY ROW_NUMBER() OVER (PARTITION BY merchant ORDER BY amount, id) = 1
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        amount,
        merchant,
        is_flagged,
        is_flagged_label,
        merchant_clean,
        ROUND(amount / SUM(amount) OVER (PARTITION BY merchant), 4) AS amount_share,
        ROUND(amount / SUM(amount) OVER (PARTITION BY merchant), 4) AS amount_share_2
    FROM ranked
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        amount,
        merchant,
        is_flagged,
        is_flagged_label,
        merchant_clean,
        amount_share,
        amount_share_2,
        FIRST_VALUE(amount) IGNORE NULLS OVER (PARTITION BY merchant ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_amount,
        NTILE(5) OVER w_running AS amount_share_bucket
    FROM enriched
    WINDOW w_running AS (ORDER BY amount_share DESC, id)
),
enriched_transactions AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAST_VALUE(amount_share_bucket, TRUE) OVER (PARTITION BY merchant ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_amount_share_bucket,
        NTILE(3) OVER (PARTITION BY merchant ORDER BY first_known_amount, id) AS first_known_amount_bucket,
        NTILE(10) OVER (ORDER BY amount, id) AS amount_bucket,
        DENSE_RANK() OVER (PARTITION BY merchant ORDER BY amount NULLS LAST) AS amount_dense_rank,
        LEAD(amount_share_2, 2) OVER (PARTITION BY merchant ORDER BY amount_share_2, id) AS next_amount_share_2
    FROM scored
),
scored_transactions AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(last_known_amount_share_bucket) OVER (PARTITION BY merchant ORDER BY last_known_amount_share_bucket, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS last_known_amount_share_bucket_avg_5,
        ROW_NUMBER() OVER (PARTITION BY merchant ORDER BY amount, id) AS seq_no
    FROM enriched_transactions
)
SELECT
    id,
    is_flagged_label,
    amount_share,
    first_known_amount,
    amount_share_bucket,
    last_known_amount_share_bucket,
    amount_bucket,
    next_amount_share_2,
    last_known_amount_share_bucket_avg_5
FROM scored_transactions
ORDER BY amount_bucket
