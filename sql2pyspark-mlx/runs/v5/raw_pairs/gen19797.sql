WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        start_date,
        end_date,
        monthly_fee,
        status,
        CONCAT_WS('/', plan, status) plan_status_key,
        DATE_FORMAT(end_date, 'MMM yyyy') end_date_label
    FROM subscriptions
    WHERE status = 'CANCELLED'
      AND customer_id IS NOT NULL
)
-- Window metrics over several different partitions
SELECT
    id,
    customer_id,
    start_date,
    monthly_fee,
    end_date_label,
    MAX(monthly_fee) OVER (PARTITION BY customer_id) customer_id_max_monthly_fee,
    LEAD(monthly_fee, 2) OVER (PARTITION BY status ORDER BY end_date, id) next_monthly_fee
FROM base
WHERE start_date <= '2025-05-01';
