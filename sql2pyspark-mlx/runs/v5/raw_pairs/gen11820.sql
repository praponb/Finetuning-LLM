WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        start_date,
        end_date,
        monthly_fee,
        DATE_ADD(end_date, 90) AS end_date_plus_90d
    FROM subscriptions
    WHERE start_date > TO_DATE('2023-07-14')
      AND customer_id IS NOT NULL
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(monthly_fee) OVER w_grp AS prev_monthly_fee,
        LEAD(monthly_fee) OVER w_grp AS next_monthly_fee,
        ROW_NUMBER() OVER w_grp AS event_seq,
        ROW_NUMBER() OVER (ORDER BY monthly_fee DESC, id) AS seq_no
    FROM filtered
    WINDOW w_grp AS (ORDER BY monthly_fee, id)
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY prev_monthly_fee) AS prev_monthly_fee_cume_dist,
        SUM(next_monthly_fee) OVER (ORDER BY seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_next_monthly_fee,
        AVG(seq_no) OVER (ORDER BY end_date, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS seq_no_avg_5_prior
    FROM enriched
),
with_windows_subscriptions AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(prev_monthly_fee_cume_dist) OVER (ORDER BY seq_no_avg_5_prior, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS prev_monthly_fee_cume_dist_avg_6,
        ROW_NUMBER() OVER (ORDER BY seq_no DESC, id) AS row_seq
    FROM with_windows
)
SELECT
    id,
    start_date,
    end_date,
    monthly_fee,
    prev_monthly_fee,
    next_monthly_fee,
    prev_monthly_fee_cume_dist,
    cumulative_next_monthly_fee,
    prev_monthly_fee_cume_dist_avg_6
FROM with_windows_subscriptions
