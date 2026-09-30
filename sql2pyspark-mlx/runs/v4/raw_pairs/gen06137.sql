WITH cleaned AS (
    SELECT
        *,
        CASE WHEN status IN ('CANCELLED', 'ACTIVE') THEN 'Group A' ELSE 'Other' END AS status_group,
        ADD_MONTHS(start_date, 3) AS start_date_plus_months
    FROM subscriptions
    WHERE end_date > DATE '2024-04-05'
),
ranked AS (
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY status ORDER BY start_date_plus_months DESC NULLS LAST) AS rn
        FROM cleaned
    ) ranked
    WHERE rn = 1
),
scored AS (
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        status_group,
        start_date_plus_months,
        rn,
        LAST_VALUE(monthly_fee) IGNORE NULLS OVER (PARTITION BY status ORDER BY monthly_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_monthly_fee,
        AVG(monthly_fee) OVER (PARTITION BY customer_id, status ORDER BY rn, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS monthly_fee_avg_6_prior,
        SUM(monthly_fee) OVER (PARTITION BY plan ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_monthly_fee,
        ROW_NUMBER() OVER (PARTITION BY customer_id, status ORDER BY start_date_plus_months DESC, id) AS event_seq,
        SUM(monthly_fee) OVER (PARTITION BY status ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_monthly_fee_2
    FROM ranked
)
SELECT *
FROM (
    SELECT
        id,
        plan,
        monthly_fee,
        last_known_monthly_fee
    FROM scored
) src
UNPIVOT INCLUDE NULLS (
    value FOR metric_name IN (monthly_fee, last_known_monthly_fee)
)
