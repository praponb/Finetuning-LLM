WITH with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        RANK() OVER w_running amount_rank,
        RANK() OVER w_running_2 amount_rank_2,
        ROW_NUMBER() OVER w_recent seq_no
    FROM payments
    WINDOW w_running AS (PARTITION BY method ORDER BY amount DESC NULLS FIRST), w_running_2 AS (PARTITION BY status ORDER BY amount DESC NULLS FIRST), w_recent AS (ORDER BY paid_at DESC, id)
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        a.*,
        b.order_date,
        b.discount,
        b.channel
    FROM with_windows a
    JOIN orders b
        ON a.order_id = b.id
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        amount_rank,
        amount_rank_2,
        seq_no,
        order_date,
        discount,
        channel,
        LAST_VALUE(channel) IGNORE NULLS OVER (PARTITION BY channel, order_id ORDER BY amount_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_channel,
        SUM(discount) OVER (PARTITION BY channel ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_discount
    FROM with_dims
),
joined AS (
    -- Attach orders attributes
    SELECT a.*, d.order_date AS order_order_date
    FROM metrics a
    JOIN orders d
        ON a.order_id = d.id
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY amount_rank_2 DESC, id) AS dedup_rank
        FROM joined
    ) ranked
    WHERE dedup_rank = 1
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        PERCENT_RANK() OVER (ORDER BY seq_no NULLS LAST) seq_no_percent_rank,
        LAG(seq_no, 2) OVER (PARTITION BY status ORDER BY paid_at, id) prev_seq_no
    FROM ranked
),
summary AS (
    -- Multi-level aggregation
    SELECT
        DATE_TRUNC('WEEK', order_order_date) AS week_start,
        APPROX_PERCENTILE(running_discount, 0.5) AS running_discount_p50,
        ROUND(SUM(amount_rank_2) / NULLIF(COUNT(CASE WHEN status = 'FAILED' THEN 1 END), 0), 2) AS amount_rank_2_per_failed,
        APPROX_PERCENTILE(discount, ARRAY(0.25, 0.5, 0.75)) AS discount_quartiles
    FROM enriched
    GROUP BY week_start
)
SELECT week_start, amount_rank_2_per_failed
FROM summary
ORDER BY amount_rank_2_per_failed DESC;
