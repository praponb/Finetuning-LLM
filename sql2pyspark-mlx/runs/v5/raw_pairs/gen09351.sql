WITH filtered AS (
    SELECT
        id,
        customer_id,
        plan,
        end_date,
        monthly_fee,
        status,
        MONTH(start_date) start_date_month,
        DATEDIFF(start_date, end_date) days_end_date_to_start_date
    FROM subscriptions
    WHERE end_date BETWEEN '2024-02-01' AND '2025-04-01'
      AND customer_id IS NOT NULL
),
latest AS (
    SELECT *
    FROM filtered
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status, plan ORDER BY start_date_month DESC NULLS FIRST, id) = 1
),
deduped AS (
    SELECT
        id,
        end_date,
        monthly_fee,
        status,
        start_date_month,
        days_end_date_to_start_date
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id, status ORDER BY monthly_fee DESC, id) AS rank_in_group
        FROM latest
    ) x
    WHERE rank_in_group = 1
),
kept_keys AS (
    SELECT status, id
    FROM deduped
    EXCEPT ALL
    SELECT status, id
    FROM subscriptions
    WHERE status = 'PAUSED'
),
reconciled AS (
    SELECT s.*
    FROM deduped s
    JOIN kept_keys k ON s.status = k.status AND s.id = k.id
),
valid AS (
    SELECT
        *,
        ROUND(days_end_date_to_start_date * 1.1, 2) days_end_date_to_start_date_scaled,
        start_date_month / NULLIF(days_end_date_to_start_date, 0) start_date_month_per_days_end_date_to_start_date
    FROM reconciled
    WHERE end_date >= TO_DATE('2023-07-18')
),
scored AS (
    SELECT
        id,
        end_date,
        monthly_fee,
        start_date_month,
        days_end_date_to_start_date_scaled,
        start_date_month_per_days_end_date_to_start_date,
        AVG(days_end_date_to_start_date_scaled) OVER (PARTITION BY status) status_avg_days_end_date_to_start_date_scaled
    FROM valid
    WHERE days_end_date_to_start_date BETWEEN 39 AND 79
),
cleaned AS (
    SELECT
        id,
        end_date,
        monthly_fee,
        days_end_date_to_start_date_scaled,
        status_avg_days_end_date_to_start_date_scaled,
        LEAST(status_avg_days_end_date_to_start_date_scaled, monthly_fee) least_status_avg_days_end_date_to_start_date_scaled_monthly_fee,
        CAST(status_avg_days_end_date_to_start_date_scaled AS STRING) status_avg_days_end_date_to_start_date_scaled_num
    FROM scored
    WHERE end_date > DATE '2023-08-07'
)
SELECT
    days_end_date_to_start_date_scaled,
    least_status_avg_days_end_date_to_start_date_scaled_monthly_fee,
    status_avg_days_end_date_to_start_date_scaled_num
FROM cleaned
