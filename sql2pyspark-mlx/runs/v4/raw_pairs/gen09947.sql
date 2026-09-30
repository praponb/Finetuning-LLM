WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        CASE WHEN monthly_fee >= 194 THEN 'High' WHEN monthly_fee >= 109 THEN 'Medium' ELSE 'Low' END AS monthly_fee_band
    FROM subscriptions
    WHERE end_date >= TO_DATE('2023-11-08')
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY end_date ASC, id ASC) AS event_seq,
        SUM(monthly_fee) OVER w_grp AS running_monthly_fee
    FROM cleaned
    WINDOW w_grp AS (PARTITION BY plan ORDER BY monthly_fee ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
cleaned_subscriptions AS (
    -- Keep recent, valid rows
    SELECT
        id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        event_seq,
        running_monthly_fee,
        CAST(monthly_fee AS INT) AS monthly_fee_int
    FROM scored
    WHERE start_date > TO_DATE('2023-09-12')
)
-- Grouped metrics
SELECT
    DATE_TRUNC('MONTH', end_date) AS activity_month,
    SUM(CASE WHEN plan = 'enterprise' THEN event_seq ELSE 0 END) AS enterprise_event_seq,
    COUNT(*) FILTER (WHERE status = 'CANCELLED') AS cancelled_rows
FROM cleaned_subscriptions
GROUP BY 1
ORDER BY cancelled_rows DESC NULLS LAST
