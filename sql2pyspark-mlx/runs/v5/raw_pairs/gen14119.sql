WITH scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        order_date,
        status,
        channel,
        amount,
        discount,
        shipping_fee,
        SUM(discount) OVER w_ordered AS cumulative_discount,
        NTILE(4) OVER (PARTITION BY store_id ORDER BY discount DESC, id ASC) AS discount_bucket,
        ROW_NUMBER() OVER (PARTITION BY channel ORDER BY discount DESC, id ASC) AS row_seq
    FROM orders
    WINDOW w_ordered AS (PARTITION BY status, store_id ORDER BY shipping_fee ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY order_date DESC, id ASC) AS rank_in_group
    FROM scored
    QUALIFY rank_in_group = 1
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, TRIM(channel) AS clean_channel
    FROM ranked
    WHERE channel IS NOT NULL
      AND order_date > TO_DATE('2023-06-10')
),
latest_match AS (
    -- Latest matching payment per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.amount,
        s.discount,
        s.discount_bucket,
        s.row_seq,
        s.rank_in_group,
        s.clean_channel,
        x.payment_status
    FROM filtered s
    JOIN LATERAL (
            SELECT c.status AS payment_status
            FROM payments c
            WHERE c.order_id = s.id
              AND c.paid_at <= s.order_date
            ORDER BY c.paid_at DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LEAST(amount, discount) AS least_amount_discount,
        rank_in_group + discount AS rank_in_group_plus_discount,
        COALESCE(payment_status, 'OTHER') AS payment_status_clean
    FROM latest_match
    WHERE payment_status IS NOT NULL
      AND discount <= 0.27
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id
    FROM base
    EXCEPT
    SELECT id
    FROM orders
    WHERE amount > 460
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    INNER JOIN remaining_keys k ON s.id = k.id
),
daily AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN row_seq >= 9 THEN 'HIGH' ELSE 'LOW' END AS row_seq_tier,
        AVG(amount) FILTER (WHERE payment_status = 'SUCCESS') AS success_avg_amount,
        COUNT(DISTINCT payment_status) AS distinct_payment_status,
        MAX(row_seq) FILTER (WHERE payment_status = 'REFUNDED') AS refunded_max_row_seq,
        MAX(discount_bucket) FILTER (WHERE payment_status = 'SUCCESS') AS success_max_discount_bucket
    FROM kept_rows
    GROUP BY CASE WHEN row_seq >= 9 THEN 'HIGH' ELSE 'LOW' END
    HAVING success_max_discount_bucket >= 0
)
SELECT
    row_seq_tier,
    success_avg_amount,
    distinct_payment_status
FROM daily
ORDER BY row_seq_tier ASC
