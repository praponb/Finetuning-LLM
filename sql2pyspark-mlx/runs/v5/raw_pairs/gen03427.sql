WITH enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(balance) OVER (ORDER BY balance, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) balance_avg_3_prior,
        ROW_NUMBER() OVER (ORDER BY opened_date DESC, id) seq_no,
        LAG(balance, 1) OVER (ORDER BY opened_date, id) prev_balance
    FROM accounts
),
enriched_accounts AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        opened_date,
        balance_avg_3_prior,
        prev_balance,
        FIRST_VALUE(balance, TRUE) OVER (PARTITION BY status ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_balance,
        ROW_NUMBER() OVER (PARTITION BY status, customer_id ORDER BY opened_date, id) row_seq
    FROM enriched
    WHERE balance_avg_3_prior <= 65323
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        MAX(balance_avg_3_prior) OVER (PARTITION BY customer_id ORDER BY first_known_balance, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) last4_max_balance_avg_3_prior,
        MIN(balance_avg_3_prior) OVER (PARTITION BY customer_id ORDER BY first_known_balance, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) last4_min_balance_avg_3_prior,
        COUNT(balance_avg_3_prior) OVER (PARTITION BY customer_id ORDER BY first_known_balance, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) last4_count_balance_avg_3_prior
    FROM enriched_accounts
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM scored s
    LEFT JOIN subscriptions sub
        ON s.customer_id = sub.customer_id
       AND s.opened_date BETWEEN sub.start_date AND sub.end_date
)
-- Ranking and gap features
SELECT
    id,
    opened_date,
    balance_avg_3_prior,
    prev_balance,
    first_known_balance,
    row_seq,
    last4_max_balance_avg_3_prior,
    last4_min_balance_avg_3_prior,
    last4_count_balance_avg_3_prior,
    subscription_id,
    monthly_fee,
    MAX(balance_avg_3_prior) OVER w_ordered running_max_balance_avg_3_prior,
    SUM(balance_avg_3_prior) OVER w_ordered running_sum_balance_avg_3_prior,
    MIN(balance_avg_3_prior) OVER w_ordered running_min_balance_avg_3_prior,
    AVG(balance_avg_3_prior) OVER w_ordered running_avg_balance_avg_3_prior
FROM with_window
WINDOW w_ordered AS (PARTITION BY customer_id ORDER BY opened_date, id, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
