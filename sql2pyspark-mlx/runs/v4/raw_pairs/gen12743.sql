WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LAST_DAY(end_date) end_date_month_end
    FROM subscriptions
    WHERE start_date >= DATE '2024-04-21'
      AND customer_id IS NOT NULL
      AND MONTH(end_date) IN (5, 6, 11)
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        plan,
        end_date,
        monthly_fee,
        status,
        end_date_month_end,
        CEIL(monthly_fee) monthly_fee_whole
    FROM base
    WHERE end_date > TO_DATE('2023-05-28')
      AND customer_id IS NOT NULL
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(monthly_fee) OVER (ORDER BY end_date_month_end, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_monthly_fee,
        ROW_NUMBER() OVER (ORDER BY end_date_month_end DESC, id) row_seq,
        AVG(monthly_fee) OVER (PARTITION BY status, plan ORDER BY end_date, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) monthly_fee_avg_5_prior,
        FIRST_VALUE(monthly_fee_whole) IGNORE NULLS OVER (PARTITION BY status ORDER BY monthly_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_monthly_fee_whole
    FROM valid
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY plan, status ORDER BY cumulative_monthly_fee DESC, id) AS row_num
    FROM scored
    QUALIFY row_num = 1
)
-- Ranking and gap features
SELECT
    id,
    plan,
    end_date,
    monthly_fee,
    status,
    end_date_month_end,
    monthly_fee_whole,
    cumulative_monthly_fee,
    row_seq,
    monthly_fee_avg_5_prior,
    first_known_monthly_fee_whole,
    row_num,
    NTILE(3) OVER (PARTITION BY plan, status ORDER BY first_known_monthly_fee_whole, id) first_known_monthly_fee_whole_bucket,
    STDDEV_SAMP(first_known_monthly_fee_whole) OVER (PARTITION BY status) status_stddev_samp_first_known_monthly_fee_whole,
    LAG(monthly_fee_whole, 2) OVER (PARTITION BY status, plan ORDER BY row_num, id) prev_monthly_fee_whole,
    AVG(row_seq) OVER (PARTITION BY plan) plan_avg_row_seq,
    LAST_VALUE(first_known_monthly_fee_whole, TRUE) OVER (PARTITION BY status ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_first_known_monthly_fee_whole
FROM deduped;
