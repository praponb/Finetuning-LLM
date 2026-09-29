WITH with_gaps AS (
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) + INTERVAL 90 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM payments
),
sessionized AS (
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
),
scored AS (
    SELECT
        id,
        order_id,
        paid_at,
        status,
        session_start_flag,
        LAG(amount, 2) OVER w_running AS prev_amount,
        FIRST_VALUE(amount, TRUE) OVER (PARTITION BY order_id ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_amount,
        SUM(session_start_flag) OVER (ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS session_start_flag_sum_7d,
        CUME_DIST() OVER w_hist AS session_no_cume_dist,
        AVG(amount) OVER (PARTITION BY status ORDER BY amount, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS amount_avg_5_prior
    FROM sessionized
    WINDOW w_running AS (ORDER BY session_no, id), w_hist AS (ORDER BY session_no)
),
enriched AS (
    SELECT
        *,
        FIRST_VALUE(first_known_amount, TRUE) OVER (PARTITION BY status, order_id ORDER BY prev_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_first_known_amount,
        SUM(prev_amount) OVER (ORDER BY session_start_flag, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_prev_amount
    FROM scored
    WHERE session_start_flag_sum_7d IS NULL
),
with_dims AS (
    SELECT
        a.*,
        p.status AS order_status,
        p.shipping_fee,
        p.amount
    FROM enriched a
    LEFT JOIN orders p
        ON a.order_id = p.id
)
SELECT
    p.channel,
    src.order_status,
    COUNT(*) AS num_rows,
    MAX(src.session_no_cume_dist) AS max_session_no_cume_dist,
    MIN(src.running_prev_amount) AS min_running_prev_amount
FROM with_dims src
LEFT JOIN orders p ON src.order_id = p.id
GROUP BY GROUPING SETS ((p.channel, src.order_status), (p.channel))
ORDER BY max_session_no_cume_dist NULLS FIRST;
