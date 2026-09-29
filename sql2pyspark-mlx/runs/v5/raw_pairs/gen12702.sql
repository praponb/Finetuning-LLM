WITH cleaned AS (
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        LAST_DAY(end_date) end_date_month_end
    FROM subscriptions
    WHERE status IN ('EXPIRED', 'ACTIVE', 'PAUSED')
),
enriched AS (
    SELECT
        *,
        LEAD(monthly_fee) OVER (PARTITION BY plan ORDER BY monthly_fee, id) next_monthly_fee,
        ROW_NUMBER() OVER (PARTITION BY plan ORDER BY monthly_fee, id) event_seq,
        NTILE(4) OVER (PARTITION BY status ORDER BY monthly_fee, id) monthly_fee_bucket
    FROM cleaned
),
enriched_subscriptions AS (
    SELECT
        id,
        customer_id,
        end_date,
        monthly_fee,
        status,
        end_date_month_end,
        next_monthly_fee,
        RANK() OVER (PARTITION BY status ORDER BY next_monthly_fee DESC) next_monthly_fee_rank,
        ROW_NUMBER() OVER (ORDER BY monthly_fee_bucket, id) row_seq,
        RANK() OVER (PARTITION BY status ORDER BY next_monthly_fee DESC) next_monthly_fee_rank_2
    FROM enriched
    WHERE next_monthly_fee IS NOT NULL
),
filtered AS (
    SELECT *, CEIL(next_monthly_fee_rank) next_monthly_fee_rank_whole
    FROM enriched_subscriptions
    WHERE YEAR(end_date) = 2024
),
by_type AS (
    SELECT *
    FROM (
        SELECT
            customer_id,
            status,
            monthly_fee
        FROM filtered
    ) src
    PIVOT (
        SUM(monthly_fee)
        FOR status IN ('EXPIRED' AS expired, 'ACTIVE' AS active)
    )
)
SELECT customer_id, expired
FROM by_type
ORDER BY customer_id DESC NULLS LAST;
