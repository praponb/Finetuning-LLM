WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        COALESCE(monthly_fee, 0) AS monthly_fee_filled
    FROM subscriptions
    WHERE customer_id IS NOT NULL
),
joined AS (
    -- Attach customers attributes
    SELECT
        s.*,
        d.signup_date,
        d.credit_limit,
        d.age
    FROM base s
    LEFT JOIN customers d
        ON s.customer_id = d.id
    WHERE d.country = 'SG'
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY signup_date DESC NULLS LAST, id) AS dedup_rank
    FROM joined
    QUALIFY dedup_rank = 1
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        monthly_fee_filled,
        signup_date,
        credit_limit,
        age,
        NTILE(3) OVER (PARTITION BY customer_id ORDER BY monthly_fee_filled DESC, id) AS monthly_fee_filled_bucket,
        NTILE(3) OVER w_ordered AS monthly_fee_filled_bucket_2
    FROM deduped
    WINDOW w_ordered AS (PARTITION BY customer_id ORDER BY monthly_fee_filled, id)
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id
    FROM scored
    EXCEPT DISTINCT
    SELECT id
    FROM subscriptions
    WHERE end_date <= '2025-09-01'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM scored s
    INNER JOIN remaining_keys k ON s.id = k.id
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('MONTH', signup_date) AS activity_month,
    ROUND(AVG(monthly_fee_filled), 2) AS avg_monthly_fee_filled,
    ROUND(AVG(monthly_fee_filled_bucket), 2) AS avg_monthly_fee_filled_bucket,
    APPROX_PERCENTILE(credit_limit, 0.95) AS credit_limit_p95,
    ROUND(AVG(monthly_fee_filled), 2) AS avg_monthly_fee_filled_2,
    PERCENTILE_APPROX(monthly_fee_filled, 0.9) AS monthly_fee_filled_p90
FROM reconciled
GROUP BY DATE_TRUNC('MONTH', signup_date)
HAVING avg_monthly_fee_filled_bucket < 1000000
