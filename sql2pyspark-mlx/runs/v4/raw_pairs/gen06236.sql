WITH latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM subscriptions
    QUALIFY ROW_NUMBER() OVER (PARTITION BY plan ORDER BY end_date DESC, id) = 1
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(monthly_fee) OVER (PARTITION BY plan ORDER BY monthly_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_monthly_fee,
        NTILE(5) OVER (PARTITION BY customer_id ORDER BY monthly_fee DESC, id) AS monthly_fee_bucket
    FROM latest
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id, plan
    FROM enriched
    EXCEPT
    SELECT id, plan
    FROM subscriptions
    WHERE plan <> 'basic'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    JOIN remaining_keys k ON s.id = k.id AND s.plan = k.plan
),
joined AS (
    -- Attach customers attributes
    SELECT
        a.*,
        b.age,
        b.name,
        b.email
    FROM kept_rows a
    LEFT JOIN customers b
        ON a.customer_id = b.id
)
SELECT
    id,
    plan,
    start_date,
    end_date,
    monthly_fee,
    status,
    cumulative_monthly_fee,
    monthly_fee_bucket,
    age
FROM joined
ORDER BY id DESC;
