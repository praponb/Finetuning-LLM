WITH enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(amount) OVER w_ordered AS prev_amount,
        ROW_NUMBER() OVER w_ordered AS seq_no
    FROM payments
    WINDOW w_ordered AS (PARTITION BY status, method ORDER BY paid_at, id)
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) > INTERVAL '90' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM enriched
),
sessions_payments AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        order_id,
        method,
        seq_no,
        is_new_session,
        LEAD(is_new_session) OVER (PARTITION BY method ORDER BY is_new_session, id) AS next_is_new_session,
        LAG(is_new_session) OVER (PARTITION BY method ORDER BY is_new_session, id) AS prev_is_new_session,
        LAG(seq_no) OVER (PARTITION BY method ORDER BY is_new_session, id) AS prev_seq_no,
        ROW_NUMBER() OVER (PARTITION BY method ORDER BY is_new_session, id) AS row_seq
    FROM sessions_payments
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY method ORDER BY is_new_session DESC, id) AS dedup_rank
    FROM scored
    QUALIFY dedup_rank = 1
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id, method ORDER BY prev_is_new_session DESC NULLS LAST, id) AS rn
    FROM ranked
    QUALIFY rn = 1
)
SELECT
    s.id,
    s.order_id,
    s.method,
    s.is_new_session,
    s.next_is_new_session,
    s.prev_is_new_session,
    s.prev_seq_no,
    s.dedup_rank,
    s.rn,
    (SELECT AVG(t2.cost) FROM shipments t2 WHERE t2.order_id = s.order_id) AS avg_shipment_cost
FROM deduped s
ORDER BY is_new_session DESC;
