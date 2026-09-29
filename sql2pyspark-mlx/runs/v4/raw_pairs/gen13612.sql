WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        COALESCE(monthly_fee, 0) AS monthly_fee_filled,
        DATE_FORMAT(start_date, 'yyyy-MM-dd') AS start_date_label
    FROM subscriptions
    WHERE customer_id IS NOT NULL
      AND customer_id IS NOT NULL
),
kept_keys AS (
    -- Set intersection on keys
    SELECT plan, customer_id
    FROM valid
    INTERSECT ALL
    SELECT plan, customer_id
    FROM subscriptions
    WHERE customer_id IS NOT NULL
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM valid s
    INNER JOIN kept_keys k ON s.plan = k.plan AND s.customer_id = k.customer_id
),
filtered AS (
    -- Keep recent, valid rows
    SELECT *, COALESCE(monthly_fee_filled, 0) AS monthly_fee_filled_filled
    FROM verified
    WHERE monthly_fee BETWEEN 70 AND 100
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        monthly_fee_filled,
        start_date_label,
        monthly_fee_filled_filled,
        DENSE_RANK() OVER (PARTITION BY plan ORDER BY monthly_fee_filled DESC) AS monthly_fee_filled_dense_rank,
        RANK() OVER w_hist AS monthly_fee_filled_filled_rank
    FROM filtered
    WINDOW w_hist AS (PARTITION BY customer_id ORDER BY monthly_fee_filled_filled DESC)
)
SELECT
    id,
    customer_id,
    plan,
    start_date,
    end_date,
    monthly_fee,
    status,
    start_date_label,
    monthly_fee_filled_dense_rank
FROM scored
ORDER BY plan DESC NULLS LAST
