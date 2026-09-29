WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        end_date,
        monthly_fee,
        status,
        DAYOFWEEK(start_date) AS start_date_dayofweek
    FROM subscriptions
    WHERE end_date > DATE '2023-05-27'
      AND start_date BETWEEN '2024-05-02' AND '2025-10-01'
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        end_date,
        monthly_fee,
        status,
        start_date_dayofweek,
        CEIL(start_date_dayofweek) AS start_date_dayofweek_whole,
        ROUND(monthly_fee, 0) AS monthly_fee_rounded
    FROM valid
    WHERE end_date > DATE '2023-08-05'
      AND monthly_fee BETWEEN 110 AND 120
),
valid_subscriptions AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CAST(monthly_fee_rounded AS INT) AS monthly_fee_rounded_int
    FROM filtered
    WHERE end_date > DATE '2023-06-02'
      AND customer_id IS NOT NULL
),
joined AS (
    -- Attach customers attributes
    SELECT
        e.*,
        b.city,
        b.name
    FROM valid_subscriptions e
    LEFT JOIN customers b
        ON e.customer_id = b.id
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        customer_id,
        status,
        city,
        monthly_fee_rounded_int
    FROM joined
) src
PIVOT (
    AVG(monthly_fee_rounded_int) AS value_sum,
    COUNT(1) AS cnt
    FOR city IN ('London', 'Singapore')
)
