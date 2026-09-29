WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        CEIL(monthly_fee) monthly_fee_whole,
        SUBSTRING(status, 1, 2) status_prefix
    FROM subscriptions
    WHERE customer_id IS NOT NULL
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        start_date,
        monthly_fee,
        status,
        monthly_fee_whole,
        status_prefix,
        ROUND(monthly_fee_whole / monthly_fee, 2) monthly_fee_whole_per_monthly_fee
    FROM valid
    WHERE start_date > DATE '2023-04-08'
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        monthly_fee,
        status,
        monthly_fee_whole,
        MAX(monthly_fee) OVER (PARTITION BY status, customer_id ORDER BY start_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) last3_max_monthly_fee,
        AVG(monthly_fee) OVER (PARTITION BY status, customer_id ORDER BY start_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) last3_avg_monthly_fee,
        SUM(monthly_fee) OVER (PARTITION BY status, customer_id ORDER BY start_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) last3_sum_monthly_fee
    FROM cleaned
),
joined AS (
    -- Attach customers attributes
    SELECT
        src.id,
        src.customer_id,
        src.monthly_fee,
        src.status,
        src.monthly_fee_whole,
        src.last3_max_monthly_fee,
        src.last3_sum_monthly_fee,
        p.city
    FROM with_windows src
    LEFT JOIN customers p
        ON src.customer_id = p.id
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        city,
        monthly_fee,
        last3_sum_monthly_fee
    FROM joined
) src
UNPIVOT (
    reading FOR measure IN (monthly_fee, last3_sum_monthly_fee)
) long_rows;
