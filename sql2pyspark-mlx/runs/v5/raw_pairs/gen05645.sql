WITH enriched AS (
    SELECT
        *,
        AVG(monthly_fee) OVER (PARTITION BY customer_id) AS customer_id_avg_monthly_fee,
        ROW_NUMBER() OVER (ORDER BY monthly_fee DESC, id) AS row_seq
    FROM subscriptions
),
ranked AS (
    SELECT *, RANK() OVER (PARTITION BY customer_id ORDER BY end_date DESC NULLS LAST) AS rn
    FROM enriched
    QUALIFY rn = 1
),
scored AS (
    SELECT
        *,
        MAX(row_seq) OVER w_recent AS last4_max_row_seq,
        MIN(row_seq) OVER w_recent AS last4_min_row_seq,
        AVG(row_seq) OVER w_recent AS last4_avg_row_seq
    FROM ranked
    WINDOW w_recent AS (PARTITION BY customer_id ORDER BY rn, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),
enriched_subscriptions AS (
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        customer_id_avg_monthly_fee,
        rn,
        last4_min_row_seq,
        LAG(customer_id_avg_monthly_fee, 1) OVER (PARTITION BY customer_id ORDER BY start_date, id) AS prev_customer_id_avg_monthly_fee,
        PERCENT_RANK() OVER (ORDER BY last4_max_row_seq DESC) AS last4_max_row_seq_percent_rank
    FROM scored
    WHERE MONTH(start_date) IN (7, 10, 11)
),
latest AS (
    SELECT
        id,
        customer_id,
        end_date,
        monthly_fee,
        last4_min_row_seq,
        prev_customer_id_avg_monthly_fee,
        last4_max_row_seq_percent_rank
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY plan, status ORDER BY start_date DESC, id) AS rn_2
        FROM enriched_subscriptions
    ) x
    WHERE rn_2 = 1
),
base AS (
    SELECT
        id,
        end_date,
        monthly_fee,
        last4_max_row_seq_percent_rank,
        FLOOR(last4_max_row_seq_percent_rank) AS last4_max_row_seq_percent_rank_whole,
        prev_customer_id_avg_monthly_fee - last4_min_row_seq AS prev_customer_id_avg_monthly_fee_minus_last4_min_row_seq
    FROM latest
    WHERE customer_id = 40
),
filtered AS (
    SELECT *, monthly_fee + prev_customer_id_avg_monthly_fee_minus_last4_min_row_seq AS monthly_fee_plus_prev_customer_id_avg_monthly_fee_minus_last4_min_row_seq
    FROM base
    WHERE prev_customer_id_avg_monthly_fee_minus_last4_min_row_seq <= 28
)
SELECT *
FROM (
    SELECT
        id,
        monthly_fee_plus_prev_customer_id_avg_monthly_fee_minus_last4_min_row_seq,
        last4_max_row_seq_percent_rank,
        prev_customer_id_avg_monthly_fee_minus_last4_min_row_seq
    FROM filtered
) src
UNPIVOT (
    metric_value FOR metric IN (monthly_fee_plus_prev_customer_id_avg_monthly_fee_minus_last4_min_row_seq AS MONTHLY_FEE_PLUS_PREV_CUSTOMER_ID_AVG_MONTHLY_FEE_MINUS_LAST4_MIN_ROW_SEQ, last4_max_row_seq_percent_rank AS LAST4_MAX_ROW_SEQ_PERCENT_RANK_VAL, prev_customer_id_avg_monthly_fee_minus_last4_min_row_seq AS PREV_CUSTOMER_ID_AVG_MONTHLY_FEE_MINUS_LAST4_MIN_ROW_SEQ_VAL)
)
