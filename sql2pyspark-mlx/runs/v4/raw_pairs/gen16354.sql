WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        CAST(monthly_fee AS INT) AS monthly_fee_int,
        ADD_MONTHS(end_date, 6) AS end_date_plus_months
    FROM subscriptions
    WHERE end_date >= TO_DATE('2024-02-06')
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(monthly_fee, 1) OVER (PARTITION BY customer_id, plan ORDER BY start_date, id) AS next_monthly_fee,
        AVG(monthly_fee_int) OVER w_part AS monthly_fee_int_avg_6
    FROM valid
    WINDOW w_part AS (PARTITION BY customer_id, plan ORDER BY end_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN monthly_fee_int_avg_6 >= 81 THEN 'High' WHEN monthly_fee_int_avg_6 >= 14 THEN 'Medium' ELSE 'Low' END AS monthly_fee_int_avg_6_tier,
        DATE_SUB(start_date, 8) AS start_date_minus_8d
    FROM scored
    WHERE start_date > TO_DATE('2023-10-07')
      AND customer_id IS NOT NULL
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY monthly_fee_int_avg_6 NULLS FIRST, id) AS rank_in_group
        FROM base
    ) ranked
    WHERE rank_in_group = 1
),
deduped_subscriptions AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM deduped
    QUALIFY ROW_NUMBER() OVER (PARTITION BY plan ORDER BY start_date, id) = 1
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        plan,
        end_date,
        monthly_fee,
        monthly_fee_int,
        end_date_plus_months,
        next_monthly_fee,
        monthly_fee_int_avg_6,
        monthly_fee_int_avg_6_tier,
        start_date_minus_8d,
        rank_in_group,
        monthly_fee_int_avg_6 + next_monthly_fee AS monthly_fee_int_avg_6_plus_next_monthly_fee,
        monthly_fee_int * monthly_fee AS monthly_fee_int_times_monthly_fee
    FROM deduped_subscriptions
    WHERE end_date >= DATE '2024-02-17'
      AND customer_id IS NOT NULL
      AND monthly_fee_int_avg_6 < 61
)
SELECT
    id,
    end_date,
    monthly_fee,
    end_date_plus_months,
    next_monthly_fee,
    monthly_fee_int_avg_6,
    monthly_fee_int_avg_6_tier,
    monthly_fee_int_avg_6_plus_next_monthly_fee,
    monthly_fee_int_times_monthly_fee
FROM filtered
ORDER BY id DESC NULLS LAST
