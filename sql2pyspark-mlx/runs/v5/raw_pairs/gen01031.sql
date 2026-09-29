WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        balance,
        opened_date,
        status,
        ADD_MONTHS(opened_date, 6) AS opened_date_plus_months,
        SUBSTRING(status, 1, 2) AS status_prefix
    FROM accounts
    WHERE customer_id IS NOT NULL
      AND status IS NOT DISTINCT FROM 'CLOSED'
),
with_latest AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.transaction_merchant
    FROM cleaned s
    JOIN LATERAL (
            SELECT c.merchant AS transaction_merchant
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(3) OVER (PARTITION BY status ORDER BY balance DESC, id) AS balance_bucket,
        SUM(balance) OVER (PARTITION BY customer_id ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_balance,
        RANK() OVER (PARTITION BY status ORDER BY balance NULLS LAST) AS balance_rank,
        NTILE(10) OVER (PARTITION BY transaction_merchant, status ORDER BY balance DESC, id) AS balance_bucket_2,
        AVG(balance) OVER (PARTITION BY transaction_merchant ORDER BY opened_date_plus_months, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS balance_avg_6_prior
    FROM with_latest
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id, transaction_merchant ORDER BY balance_rank DESC NULLS LAST, id) AS rank_in_group
        FROM enriched
    ) x
    WHERE rank_in_group = 1
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM latest s
    WHERE EXISTS (
            SELECT 1
            FROM transactions x
            WHERE x.account_id = s.id
              AND x.amount >= 1500
        )
)
SELECT
    s.id,
    s.customer_id,
    s.balance,
    s.opened_date_plus_months,
    s.status_prefix,
    s.transaction_merchant,
    s.balance_bucket,
    s.cumulative_balance,
    s.balance_bucket_2,
    (SELECT MAX(t2.shipping_fee) FROM orders t2 WHERE t2.customer_id = s.customer_id) AS max_order_shipping_fee
FROM not_flagged s
