WITH valid AS (
    SELECT *, ROUND(monthly_fee, 1) AS monthly_fee_rounded
    FROM subscriptions
    WHERE start_date >= DATE '2024-03-16'
      AND customer_id IS NOT NULL
      AND plan = 'basic'
),
metrics AS (
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        monthly_fee_rounded,
        LEAD(monthly_fee_rounded, 1) OVER (ORDER BY start_date, id) AS next_monthly_fee_rounded,
        DENSE_RANK() OVER (PARTITION BY plan ORDER BY monthly_fee_rounded NULLS LAST) AS monthly_fee_rounded_dense_rank,
        LAST_VALUE(monthly_fee_rounded) IGNORE NULLS OVER (PARTITION BY plan, customer_id ORDER BY monthly_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_monthly_fee_rounded
    FROM valid
),
enriched AS (
    SELECT
        src.*,
        d.credit_limit,
        d.segment,
        d.email
    FROM metrics src
    LEFT JOIN customers d
        ON src.customer_id = d.id
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
        monthly_fee_rounded,
        next_monthly_fee_rounded,
        monthly_fee_rounded_dense_rank,
        last_known_monthly_fee_rounded,
        credit_limit,
        segment,
        email,
        NTILE(4) OVER (ORDER BY monthly_fee_rounded, id) AS monthly_fee_rounded_bucket,
        RANK() OVER (PARTITION BY plan ORDER BY monthly_fee DESC NULLS LAST) AS monthly_fee_rank,
        SUM(credit_limit) OVER (PARTITION BY customer_id ORDER BY credit_limit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_credit_limit
    FROM enriched
),
enriched_subscriptions AS (
    SELECT
        *,
        LAST_VALUE(monthly_fee_rounded_dense_rank) IGNORE NULLS OVER (PARTITION BY customer_id, status ORDER BY cumulative_credit_limit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_monthly_fee_rounded_dense_rank,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY monthly_fee_rounded, id) AS row_seq
    FROM scored
)
SELECT *
FROM (
    SELECT
        id,
        segment,
        last_known_monthly_fee_rounded,
        credit_limit
    FROM enriched_subscriptions
) src
UNPIVOT (
    reading FOR measure IN (last_known_monthly_fee_rounded, credit_limit AS CREDIT_LIMIT_VAL)
)
