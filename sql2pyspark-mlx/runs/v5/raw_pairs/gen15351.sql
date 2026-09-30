WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT *, ADD_MONTHS(start_date, 6) AS start_date_plus_months
    FROM subscriptions
    WHERE plan = 'basic'
),
matched_keys AS (
    -- Set difference on keys
    SELECT plan
    FROM cleaned
    EXCEPT
    SELECT plan
    FROM subscriptions
    WHERE status != 'PAUSED'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned s
    INNER JOIN matched_keys k ON s.plan = k.plan
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        start_date,
        end_date,
        monthly_fee,
        status,
        CUME_DIST() OVER (PARTITION BY plan ORDER BY monthly_fee NULLS FIRST) AS monthly_fee_cume_dist
    FROM kept_rows
    WHERE start_date BETWEEN '2023-04-09' AND '2024-08-01'
),
cleaned_subscriptions AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        start_date,
        end_date,
        monthly_fee,
        YEAR(start_date) AS start_date_year
    FROM metrics
    WHERE end_date > '2025-05-18'
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        start_date,
        end_date,
        monthly_fee,
        start_date_year,
        GREATEST(start_date_year, monthly_fee) AS greatest_start_date_year_monthly_fee,
        DATEDIFF(end_date, start_date) AS days_start_date_to_end_date
    FROM cleaned_subscriptions
    WHERE customer_id IS NOT NULL
)
SELECT
    customer_id,
    end_date,
    greatest_start_date_year_monthly_fee,
    days_start_date_to_end_date
FROM filtered
ORDER BY days_start_date_to_end_date DESC NULLS FIRST
