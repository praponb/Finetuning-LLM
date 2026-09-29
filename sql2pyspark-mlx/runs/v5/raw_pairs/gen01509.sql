WITH joined AS (
    SELECT
        a.*,
        b.order_date,
        b.amount,
        b.status AS order_status
    FROM shipments a
    LEFT JOIN orders b
        ON a.order_id = b.id
    WHERE b.status = 'CANCELLED'
),
latest AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id, status ORDER BY cost NULLS LAST, id) AS rank_in_group
        FROM joined
    ) ranked
    WHERE rank_in_group = 1
),
deduped AS (
    SELECT *
    FROM latest
    QUALIFY RANK() OVER (PARTITION BY status ORDER BY delivered_date DESC) = 1
),
with_windows AS (
    SELECT
        id,
        carrier,
        status,
        order_date,
        amount,
        order_status,
        LEAD(cost, 1) OVER w_ordered AS next_cost,
        NTILE(5) OVER w_ordered_2 AS cost_bucket,
        RANK() OVER (PARTITION BY order_status, order_id ORDER BY amount) AS amount_rank,
        NTILE(5) OVER (ORDER BY amount DESC, id) AS amount_bucket,
        ROUND(amount / SUM(amount) OVER (PARTITION BY order_status), 4) AS amount_share
    FROM deduped
    WINDOW w_ordered AS (PARTITION BY status ORDER BY shipped_date, id), w_ordered_2 AS (PARTITION BY order_id ORDER BY cost DESC, id)
)
SELECT *
FROM (
    SELECT
        id,
        order_status,
        amount,
        next_cost,
        amount_share
    FROM with_windows
) src
UNPIVOT INCLUDE NULLS (
    val FOR measure IN (amount AS AMOUNT, next_cost, amount_share AS AMOUNT_SHARE)
);
