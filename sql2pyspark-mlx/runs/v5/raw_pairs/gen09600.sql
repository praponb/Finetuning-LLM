WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, CAST(monthly_fee AS INT) AS monthly_fee_int
    FROM subscriptions
    WHERE end_date > TO_DATE('2023-11-07')
      AND monthly_fee BETWEEN 150 AND 190
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        s.*,
        c.credit_limit,
        c.signup_date,
        c.email
    FROM cleaned s
    INNER JOIN customers c
        ON s.customer_id = c.id
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY plan, customer_id ORDER BY signup_date DESC, id) AS rn
        FROM with_dims
    ) x
    WHERE rn = 1
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        INITCAP(TRIM(plan)) AS plan_title,
        DATE_FORMAT(signup_date, 'MMM yyyy') AS signup_date_label
    FROM ranked
    WHERE end_date >= TO_DATE('2023-07-09')
      AND customer_id IS NOT NULL
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        plan,
        credit_limit,
        monthly_fee
    FROM base
) src
UNPIVOT (
    val FOR metric_name IN (credit_limit, monthly_fee AS MONTHLY_FEE)
) u
