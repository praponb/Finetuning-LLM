WITH deduped AS (
    SELECT
        id,
        order_id,
        paid_at,
        status
    FROM payments
    QUALIFY RANK() OVER (PARTITION BY order_id ORDER BY paid_at DESC NULLS LAST) = 1
),
valid AS (
    SELECT *, TO_DATE(paid_at) AS paid_at_date
    FROM deduped
    WHERE order_id IS NOT NULL
),
gaps AS (
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) > INTERVAL '30' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM valid
),
islands AS (
    SELECT
        id,
        paid_at,
        status,
        paid_at_date,
        SUM(new_group_flag) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
),
with_gaps AS (
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) > INTERVAL '7' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM islands
),
sessions_payments AS (
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
),
metrics AS (
    SELECT
        *,
        MAX(is_new_burst) OVER (PARTITION BY status ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS is_new_burst_max_90d,
        NTILE(3) OVER w_ordered AS burst_id_bucket,
        LAST_VALUE(is_new_burst) IGNORE NULLS OVER (PARTITION BY status ORDER BY paid_at_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_is_new_burst
    FROM sessions_payments
    WINDOW w_ordered AS (PARTITION BY status ORDER BY burst_id, id)
),
latest AS (
    SELECT *
    FROM metrics
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status ORDER BY burst_id DESC, id) = 1
)
SELECT
    id,
    paid_at,
    status,
    burst_id,
    is_new_burst,
    is_new_burst_max_90d,
    burst_id_bucket
FROM latest;
