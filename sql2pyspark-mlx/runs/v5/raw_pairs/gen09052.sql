WITH base AS (
    SELECT *, DATE_SUB(end_date, 82) AS end_date_minus_82d
    FROM subscriptions
    WHERE end_date > TO_DATE('2023-08-26')
),
latest AS (
    SELECT *
    FROM base
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY end_date_minus_82d DESC NULLS LAST, id) = 1
),
deduped AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY end_date_minus_82d NULLS FIRST, id) AS latest_rank
        FROM latest
    ) t
    WHERE latest_rank = 1
),
with_window AS (
    SELECT
        src.*,
        sub.id AS subscription_id,
        sub.plan AS subscription_plan,
        sub.monthly_fee AS subscription_monthly_fee
    FROM deduped src
    INNER JOIN subscriptions sub
        ON src.customer_id = sub.customer_id
       AND src.end_date >= sub.start_date
       AND src.end_date < sub.end_date + INTERVAL 14 DAYS
)
SELECT
    id,
    customer_id,
    plan,
    start_date,
    end_date,
    monthly_fee,
    latest_rank,
    subscription_id,
    subscription_monthly_fee
FROM with_window;
