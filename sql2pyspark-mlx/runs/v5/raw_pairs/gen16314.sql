WITH ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM payments
    QUALIFY RANK() OVER (PARTITION BY order_id, status ORDER BY paid_at NULLS FIRST) = 1
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        method,
        amount,
        paid_at,
        CAST(paid_at AS DATE) AS paid_at_day,
        COALESCE(method, 'OTHER') AS method_clean
    FROM ranked
    WHERE paid_at > DATE '2023-10-01'
      AND method <> 'promptpay'
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        COUNT(amount) OVER w_grp AS running_count_amount,
        MIN(amount) OVER w_grp AS running_min_amount,
        SUM(amount) OVER w_grp AS running_sum_amount,
        LEAD(amount, 1) OVER (PARTITION BY method ORDER BY amount, id) AS next_amount
    FROM base
    WINDOW w_grp AS (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
matched_keys AS (
    -- Set difference on keys
    SELECT method, id
    FROM scored
    EXCEPT
    SELECT method, id
    FROM payments
    WHERE TO_DATE(paid_at) >= '2024-07-31'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM scored s
    INNER JOIN matched_keys k ON s.method = k.method AND s.id = k.id
)
SELECT
    id,
    method,
    amount,
    paid_at,
    running_count_amount,
    running_min_amount,
    running_sum_amount,
    next_amount
FROM verified
ORDER BY running_min_amount DESC, amount
