WITH filtered AS (
    SELECT *, ROUND(amount * 1.15, 2) adjusted_amount
    FROM transactions
    WHERE txn_time > DATE '2023-05-05'
),
with_windows AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY adjusted_amount, id) seq_no,
        NTILE(3) OVER (PARTITION BY txn_type ORDER BY amount DESC, id) amount_bucket,
        SUM(adjusted_amount) OVER (PARTITION BY merchant) merchant_sum_adjusted_amount
    FROM filtered
),
deduped AS (
    SELECT *, RANK() OVER (PARTITION BY merchant, account_id ORDER BY seq_no DESC) AS latest_rank
    FROM with_windows
    QUALIFY latest_rank = 1
),
metrics AS (
    SELECT
        id,
        account_id,
        txn_type,
        merchant,
        merchant_sum_adjusted_amount,
        SUM(merchant_sum_adjusted_amount) OVER (ORDER BY amount_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_merchant_sum_adjusted_amount,
        SUM(merchant_sum_adjusted_amount) OVER (PARTITION BY txn_type ORDER BY merchant_sum_adjusted_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_merchant_sum_adjusted_amount_2,
        SUM(amount_bucket) OVER (PARTITION BY txn_type, account_id ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_amount_bucket
    FROM deduped
),
base AS (
    SELECT
        *,
        CASE WHEN cumulative_amount_bucket >= 44 THEN 'High' WHEN cumulative_amount_bucket >= 16 THEN 'Medium' ELSE 'Low' END cumulative_amount_bucket_level,
        ROUND(cumulative_merchant_sum_adjusted_amount * 1.15, 2) cumulative_merchant_sum_adjusted_amount_scaled
    FROM metrics
    WHERE merchant IS NOT NULL
)
SELECT
    id,
    account_id,
    merchant,
    cumulative_amount_bucket,
    CUME_DIST() OVER w_ordered merchant_sum_adjusted_amount_cume_dist,
    SUM(cumulative_amount_bucket) OVER w_hist cumulative_cumulative_amount_bucket,
    AVG(cumulative_merchant_sum_adjusted_amount) OVER (PARTITION BY merchant ORDER BY cumulative_merchant_sum_adjusted_amount_2, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) cumulative_merchant_sum_adjusted_amount_avg_2
FROM base
WINDOW w_ordered AS (ORDER BY merchant_sum_adjusted_amount DESC NULLS FIRST), w_hist AS (ORDER BY cumulative_merchant_sum_adjusted_amount_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
