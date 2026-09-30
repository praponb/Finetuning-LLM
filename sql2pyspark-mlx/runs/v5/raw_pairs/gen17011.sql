WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT *, ADD_MONTHS(end_date, 1) AS end_date_plus_months
    FROM subscriptions
    WHERE status IN ('EXPIRED', 'ACTIVE')
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM filtered
    QUALIFY RANK() OVER (PARTITION BY plan ORDER BY start_date ASC NULLS FIRST) = 1
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY plan, customer_id ORDER BY end_date_plus_months ASC, id ASC) AS row_seq,
        AVG(monthly_fee) OVER w_hist AS monthly_fee_avg_2,
        SUM(monthly_fee) OVER (PARTITION BY customer_id ORDER BY end_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_monthly_fee,
        SUM(monthly_fee) OVER (PARTITION BY plan ORDER BY end_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_monthly_fee_2
    FROM ranked
    WINDOW w_hist AS (PARTITION BY status ORDER BY end_date ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        plan,
        end_date,
        status,
        row_seq,
        running_monthly_fee,
        LAG(running_monthly_fee) OVER (PARTITION BY status ORDER BY monthly_fee_avg_2 ASC, id ASC) AS prev_running_monthly_fee,
        LAG(monthly_fee) OVER (PARTITION BY status ORDER BY monthly_fee_avg_2 ASC, id ASC) AS prev_monthly_fee,
        LEAD(monthly_fee) OVER (PARTITION BY status ORDER BY monthly_fee_avg_2 ASC, id ASC) AS next_monthly_fee,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY monthly_fee_avg_2 ASC, id ASC) AS seq_no,
        NTILE(3) OVER (PARTITION BY status ORDER BY running_monthly_fee_2 ASC, id ASC) AS running_monthly_fee_2_bucket
    FROM enriched
    WHERE row_seq > 5
)
SELECT
    s.id,
    s.customer_id,
    s.end_date,
    s.status,
    s.row_seq,
    s.running_monthly_fee,
    s.prev_running_monthly_fee,
    s.prev_monthly_fee,
    s.next_monthly_fee,
    (SELECT MIN(x.balance) FROM accounts x WHERE x.customer_id = s.customer_id) AS min_account_balance
FROM metrics s
ORDER BY prev_running_monthly_fee ASC NULLS LAST, id DESC
