WITH valid AS (
    SELECT
        id,
        account_id,
        txn_type,
        txn_time,
        merchant,
        ROUND(amount * 1.07, 2) amount_scaled,
        COALESCE(merchant, 'DIRECT') merchant_clean
    FROM transactions
    WHERE amount < 3300
),
matched_keys AS (
    SELECT id, account_id
    FROM valid
    EXCEPT
    SELECT id, account_id
    FROM transactions
    WHERE txn_type IN ('REFUND', 'CREDIT', 'DEBIT')
),
verified AS (
    SELECT s.*
    FROM valid s
    INNER JOIN matched_keys k ON s.id = k.id AND s.account_id = k.account_id
),
cleaned AS (
    SELECT *, INITCAP(TRIM(txn_type)) txn_type_title
    FROM verified
    WHERE txn_time > TIMESTAMP '2023-07-26 01:00:00'
      AND merchant IS DISTINCT FROM '7-Eleven'
),
scored AS (
    SELECT
        id,
        account_id,
        txn_type,
        merchant_clean,
        DENSE_RANK() OVER (ORDER BY amount_scaled DESC NULLS FIRST) amount_scaled_dense_rank,
        MAX(amount_scaled) OVER w_hist amount_scaled_max_30d,
        SUM(amount_scaled) OVER (ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) amount_scaled_sum_7d,
        SUM(amount_scaled) OVER (PARTITION BY merchant, txn_type) merchant_sum_amount_scaled,
        PERCENT_RANK() OVER (PARTITION BY txn_type ORDER BY amount_scaled DESC NULLS LAST) amount_scaled_percent_rank
    FROM cleaned
    WINDOW w_hist AS (PARTITION BY merchant ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW)
),
enriched AS (
    SELECT
        id,
        account_id,
        txn_type,
        amount_scaled_dense_rank,
        amount_scaled_max_30d,
        merchant_sum_amount_scaled,
        CUME_DIST() OVER (ORDER BY amount_scaled_max_30d NULLS FIRST) amount_scaled_max_30d_cume_dist,
        DENSE_RANK() OVER (ORDER BY merchant_sum_amount_scaled) merchant_sum_amount_scaled_dense_rank,
        MAX(amount_scaled_max_30d) OVER w_running txn_type_max_amount_scaled_max_30d,
        CUME_DIST() OVER w_part amount_scaled_sum_7d_cume_dist,
        RANK() OVER (PARTITION BY account_id ORDER BY amount_scaled_sum_7d NULLS LAST) amount_scaled_sum_7d_rank
    FROM scored
    WHERE account_id IN (1, 3, 33, 37)
    WINDOW w_running AS (PARTITION BY txn_type), w_part AS (PARTITION BY txn_type ORDER BY amount_scaled_sum_7d NULLS FIRST)
)
SELECT
    d.status,
    s.txn_type,
    COUNT(*) AS cnt,
    ROUND(SUM(s.amount_scaled_sum_7d_cume_dist), 2) AS total_amount_scaled_sum_7d_cume_dist,
    ROUND(AVG(s.merchant_sum_amount_scaled_dense_rank), 2) AS avg_merchant_sum_amount_scaled_dense_rank
FROM enriched s
LEFT JOIN accounts d ON s.account_id = d.id
GROUP BY GROUPING SETS ((d.status, s.txn_type), (d.status), ())
