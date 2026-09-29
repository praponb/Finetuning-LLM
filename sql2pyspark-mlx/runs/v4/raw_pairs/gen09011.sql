WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LAST_DAY(start_date) AS start_date_month_end
    FROM subscriptions
    WHERE end_date >= DATE '2023-05-12'
      AND customer_id IS NOT NULL
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        RANK() OVER (PARTITION BY status, plan ORDER BY monthly_fee NULLS FIRST) AS monthly_fee_rank,
        RANK() OVER (PARTITION BY customer_id, plan ORDER BY monthly_fee) AS monthly_fee_rank_2,
        MAX(monthly_fee) OVER (PARTITION BY customer_id) AS customer_id_max_monthly_fee
    FROM filtered
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        e.*,
        d.name,
        d.city
    FROM metrics e
    LEFT JOIN customers d
        ON e.customer_id = d.id
),
pivoted AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            city,
            status,
            monthly_fee_rank_2
        FROM with_dims
    ) src
    PIVOT (
        AVG(monthly_fee_rank_2) AS total,
        COUNT(1) AS n
        FOR status IN ('CANCELLED', 'PAUSED', 'EXPIRED')
    )
)
SELECT
    city,
    CANCELLED_total,
    PAUSED_total,
    PAUSED_n,
    EXPIRED_total
FROM pivoted;
