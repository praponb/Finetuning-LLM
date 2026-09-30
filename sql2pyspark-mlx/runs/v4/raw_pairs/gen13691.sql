WITH valid AS (
    SELECT *, CAST(paid_at AS DATE) AS paid_at_day
    FROM payments
    WHERE status != 'REFUNDED'
),
with_dims AS (
    SELECT src.*, b.amount AS order_amount
    FROM valid src
    LEFT JOIN orders b
        ON src.order_id = b.id
),
flagged AS (
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) > INTERVAL '90' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM with_dims
),
sessionized AS (
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
enriched AS (
    SELECT
        *,
        COUNT(order_amount) OVER (PARTITION BY order_id, method) AS order_id_count_order_amount,
        LAST_VALUE(order_amount, TRUE) OVER (ORDER BY paid_at_day, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_order_amount,
        AVG(amount) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS amount_avg_5_prior,
        LAST_VALUE(session_start_flag) IGNORE NULLS OVER (PARTITION BY method, order_id ORDER BY session_start_flag, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_session_start_flag,
        ROW_NUMBER() OVER w_ordered AS event_seq
    FROM sessionized
    WINDOW w_ordered AS (PARTITION BY status ORDER BY session_no DESC, id)
),
filtered AS (
    SELECT *, event_seq * session_start_flag AS event_seq_times_session_start_flag
    FROM enriched
    WHERE paid_at > TIMESTAMP '2023-05-08 05:00:00'
      AND order_id IS NOT NULL
),
base AS (
    SELECT *, TRIM(method) AS clean_method
    FROM filtered
    WHERE order_amount >= 1530
),
latest AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY paid_at DESC, id) AS rank_in_group
        FROM base
    ) t
    WHERE rank_in_group = 1
),
stats AS (
    SELECT
        order_id,
        CASE WHEN session_start_flag >= 1 THEN 'PREMIUM' ELSE 'STANDARD' END AS session_start_flag_tier,
        session_no,
        SIZE(COLLECT_SET(method)) AS num_methods,
        COUNT(*) FILTER (WHERE method = 'bank_transfer') AS bank_transfer_rows,
        AVG(last_known_order_amount) FILTER (WHERE method = 'paypal') AS paypal_avg_last_known_order_amount,
        SUM(amount_avg_5_prior) FILTER (WHERE status = 'SUCCESS') AS success_total_amount_avg_5_prior,
        DENSE_RANK() OVER (PARTITION BY order_id ORDER BY SIZE(COLLECT_SET(method)) DESC) AS num_methods_dense_rank
    FROM latest
    GROUP BY order_id, CASE WHEN session_start_flag >= 1 THEN 'PREMIUM' ELSE 'STANDARD' END, session_no
)
SELECT
    order_id,
    session_start_flag_tier,
    session_no,
    num_methods,
    bank_transfer_rows,
    paypal_avg_last_known_order_amount,
    success_total_amount_avg_5_prior
FROM stats
ORDER BY paypal_avg_last_known_order_amount DESC;
