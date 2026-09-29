WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        TRIM(status) clean_status,
        LENGTH(status) status_length
    FROM subscriptions
    WHERE end_date >= DATE '2023-08-13'
      AND status IN ('ACTIVE', 'EXPIRED')
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM valid
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status ORDER BY monthly_fee NULLS LAST, id) = 1
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        LAST_VALUE(monthly_fee, TRUE) OVER (PARTITION BY customer_id ORDER BY monthly_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_monthly_fee,
        RANK() OVER w_ordered monthly_fee_rank,
        AVG(monthly_fee) OVER (PARTITION BY plan, status ORDER BY monthly_fee, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) monthly_fee_avg_3_prior
    FROM latest
    WINDOW w_ordered AS (ORDER BY monthly_fee DESC NULLS FIRST)
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY monthly_fee, id) event_seq,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY end_date, id) row_seq,
        FIRST_VALUE(status_length, TRUE) OVER (PARTITION BY plan, customer_id ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_status_length,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY monthly_fee DESC, id) row_seq_2
    FROM enriched
)
-- Ranking and gap features
SELECT
    id,
    customer_id,
    plan,
    start_date,
    end_date,
    monthly_fee,
    status,
    clean_status,
    status_length,
    last_known_monthly_fee,
    monthly_fee_rank,
    monthly_fee_avg_3_prior,
    event_seq,
    row_seq,
    first_known_status_length,
    row_seq_2,
    SUM(first_known_status_length) OVER (ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_first_known_status_length,
    AVG(status_length) OVER w_recent status_length_avg_3
FROM metrics
WINDOW w_recent AS (ORDER BY monthly_fee_rank, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW);
