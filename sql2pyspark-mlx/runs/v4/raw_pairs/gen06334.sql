WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT *, DATE_SUB(start_date, 47) AS start_date_minus_47d
    FROM subscriptions
    WHERE start_date > TO_DATE('2023-07-12')
),
kept_keys AS (
    -- Set difference on keys
    SELECT status, id
    FROM filtered
    EXCEPT DISTINCT
    SELECT status, id
    FROM subscriptions
    WHERE monthly_fee > 180
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    INNER JOIN kept_keys k ON s.status = k.status AND s.id = k.id
),
joined AS (
    -- Attach customers attributes
    SELECT
        e.*,
        b.credit_limit,
        b.age,
        b.segment
    FROM kept_rows e
    INNER JOIN customers b
        ON e.customer_id = b.id
),
with_dims AS (
    -- Attach customers attributes
    SELECT src.*, c.city
    FROM joined src
    INNER JOIN customers c
        ON src.customer_id = c.id
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        start_date_minus_47d,
        credit_limit,
        age,
        segment,
        city,
        PERCENT_RANK() OVER (ORDER BY age NULLS LAST) AS age_percent_rank,
        MAX(monthly_fee) OVER (PARTITION BY status) AS status_max_monthly_fee
    FROM with_dims
),
scored_subscriptions AS (
    -- Ranking and gap features
    SELECT
        *,
        RANK() OVER (ORDER BY age_percent_rank NULLS FIRST) AS age_percent_rank_rank,
        COUNT(credit_limit) OVER (PARTITION BY status, city) AS status_count_credit_limit
    FROM scored
),
with_dims_subscriptions AS (
    -- Attach customers attributes
    SELECT
        e.*,
        c.segment AS customer_segment,
        c.name
    FROM scored_subscriptions e
    LEFT JOIN customers c
        ON e.customer_id = c.id
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        city,
        credit_limit,
        monthly_fee,
        age_percent_rank
    FROM with_dims_subscriptions
) src
UNPIVOT INCLUDE NULLS (
    value FOR metric IN (credit_limit AS CREDIT_LIMIT_VAL, monthly_fee AS MONTHLY_FEE_VAL, age_percent_rank AS AGE_PERCENT_RANK)
);
