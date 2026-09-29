WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LENGTH(merchant) merchant_length,
        COALESCE(merchant, 'N/A') merchant_clean
    FROM transactions
    WHERE txn_time >= DATE '2024-01-23'
      AND merchant IS NOT NULL
),
joined AS (
    -- Attach accounts attributes
    SELECT
        src.*,
        d.opened_date,
        d.balance,
        d.status
    FROM base src
    JOIN accounts d
        ON src.account_id = d.id
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        is_flagged,
        merchant_length,
        merchant_clean,
        opened_date,
        balance,
        status,
        DENSE_RANK() OVER (PARTITION BY account_id ORDER BY merchant_length DESC) merchant_length_dense_rank,
        DENSE_RANK() OVER (PARTITION BY account_id ORDER BY balance) balance_dense_rank
    FROM joined
),
summary AS (
    -- Multi-level aggregation
    SELECT
        DATE_TRUNC('YEAR', txn_time) AS year_start,
        AVG(merchant_length_dense_rank) FILTER (WHERE merchant = 'Amazon') AS amazon_avg_merchant_length_dense_rank,
        SORT_ARRAY(COLLECT_SET(merchant)) AS merchant_list,
        SUM(CASE WHEN txn_type = 'FEE' THEN amount ELSE 0 END) AS fee_amount,
        SUM(amount) AS total_amount
    FROM enriched
    GROUP BY 1
)
SELECT
    year_start,
    amazon_avg_merchant_length_dense_rank,
    fee_amount,
    total_amount
FROM summary
ORDER BY year_start DESC, fee_amount
LIMIT 10
