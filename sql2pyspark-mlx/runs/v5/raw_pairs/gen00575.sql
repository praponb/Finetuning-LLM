WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        DATE_TRUNC('YEAR', end_date) end_date_period,
        CASE WHEN monthly_fee >= 270 THEN 'High' WHEN monthly_fee >= 97 THEN 'Medium' ELSE 'Low' END monthly_fee_level
    FROM subscriptions
    WHERE status IN ('EXPIRED', 'ACTIVE', 'PAUSED')
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        a.*,
        b.email,
        b.segment
    FROM valid a
    INNER JOIN customers b
        ON a.customer_id = b.id
    WHERE b.country IN ('UK', 'DE', 'JP')
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(4) OVER (PARTITION BY status, plan ORDER BY monthly_fee DESC, id) monthly_fee_bucket,
        RANK() OVER (PARTITION BY plan ORDER BY monthly_fee DESC NULLS FIRST) monthly_fee_rank,
        AVG(monthly_fee) OVER (PARTITION BY status, segment ORDER BY end_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) monthly_fee_avg_2
    FROM with_dims
),
matched_keys AS (
    -- Set difference on keys
    SELECT customer_id
    FROM scored
    EXCEPT DISTINCT
    SELECT customer_id
    FROM subscriptions
    WHERE status IN ('ACTIVE', 'CANCELLED', 'EXPIRED')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM scored s
    INNER JOIN matched_keys k ON s.customer_id = k.customer_id
),
remaining_keys AS (
    -- Set difference on keys
    SELECT plan, status
    FROM kept_rows
    EXCEPT
    SELECT plan, status
    FROM subscriptions
    WHERE customer_id = 2
),
kept_rows_subscriptions AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM kept_rows s
    INNER JOIN remaining_keys k ON s.plan = k.plan AND s.status = k.status
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        plan,
        end_date,
        end_date_period,
        monthly_fee_level,
        email,
        segment,
        monthly_fee_bucket,
        monthly_fee_avg_2,
        ROUND(monthly_fee_avg_2 / monthly_fee_rank, 2) monthly_fee_avg_2_per_monthly_fee_rank,
        COALESCE(segment, 'NONE') resolved_segment
    FROM kept_rows_subscriptions
    WHERE segment IS DISTINCT FROM 'Corporate'
      AND customer_id IN (4, 6, 7, 32)
)
-- Grouped metrics
SELECT
    customer_id,
    DATE_TRUNC('WEEK', end_date) AS week_start,
    COUNT(*) AS cnt,
    COUNT(*) FILTER (WHERE plan = 'basic') AS basic_rows,
    COUNT(*) AS num_rows,
    ROUND(SUM(monthly_fee_avg_2) / NULLIF(COUNT(CASE WHEN segment = 'SMB' THEN 1 END), 0), 2) AS monthly_fee_avg_2_per_smb
FROM cleaned
GROUP BY customer_id, DATE_TRUNC('WEEK', end_date);
