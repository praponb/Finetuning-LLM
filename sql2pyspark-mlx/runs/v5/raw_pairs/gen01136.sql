WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        start_date,
        end_date,
        monthly_fee,
        status,
        LAST_DAY(start_date) AS start_date_month_end
    FROM subscriptions
    WHERE start_date > DATE '2023-09-26'
),
filtered_subscriptions AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        start_date,
        monthly_fee,
        start_date_month_end,
        CASE WHEN monthly_fee >= 246 THEN 'High' WHEN monthly_fee >= 85 THEN 'Medium' ELSE 'Low' END AS monthly_fee_level,
        DATE_ADD(start_date, 34) AS start_date_plus_34d
    FROM filtered
    WHERE customer_id IS NOT NULL
      AND status IN ('CANCELLED', 'EXPIRED')
),
joined AS (
    -- Attach customers attributes
    SELECT
        a.id,
        a.customer_id,
        a.start_date,
        a.monthly_fee,
        a.start_date_month_end,
        a.monthly_fee_level,
        a.start_date_plus_34d,
        p.name,
        p.age,
        p.credit_limit
    FROM filtered_subscriptions a
    LEFT JOIN customers p
        ON a.customer_id = p.id
),
with_dims AS (
    -- Attach customers attributes
    SELECT a.*, b.age AS customer_age
    FROM joined a
    JOIN customers b
        ON a.customer_id = b.id
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY start_date, id) AS dedup_rank
    FROM with_dims
    QUALIFY dedup_rank = 1
),
filtered_subscriptions_2 AS (
    -- Keep recent, valid rows
    SELECT
        *,
        credit_limit * dedup_rank AS credit_limit_times_dedup_rank,
        monthly_fee - age AS monthly_fee_minus_age
    FROM ranked
    WHERE customer_id IS NOT NULL
)
-- Native SQL UNPIVOT (wide to long)
SELECT
    id,
    customer_id,
    metric,
    val
FROM (
    SELECT
        id,
        customer_id,
        monthly_fee_minus_age,
        credit_limit_times_dedup_rank,
        credit_limit
    FROM filtered_subscriptions_2
) src
UNPIVOT (
    val FOR metric IN (monthly_fee_minus_age AS MONTHLY_FEE_MINUS_AGE, credit_limit_times_dedup_rank AS CREDIT_LIMIT_TIMES_DEDUP_RANK_VAL, credit_limit)
)
