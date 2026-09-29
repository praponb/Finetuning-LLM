WITH deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY method ORDER BY paid_at NULLS LAST, id) AS rank_in_group
    FROM payments
    QUALIFY rank_in_group = 1
),
scored AS (
    SELECT
        id,
        order_id,
        amount,
        rank_in_group,
        AVG(amount) OVER (PARTITION BY method, order_id ORDER BY rank_in_group, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_avg_amount,
        SUM(amount) OVER (PARTITION BY method, order_id ORDER BY rank_in_group, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_sum_amount,
        COUNT(amount) OVER (PARTITION BY method, order_id ORDER BY rank_in_group, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_count_amount,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY rank_in_group, id) row_seq
    FROM deduped
    WHERE amount >= 880
),
with_dims AS (
    SELECT
        a.*,
        p.order_date,
        p.amount AS order_amount
    FROM scored a
    JOIN orders p
        ON a.order_id <=> p.id
),
latest AS (
    SELECT *
    FROM with_dims
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY running_count_amount DESC NULLS LAST, id) = 1
)
SELECT
    o.status,
    COUNT(*) AS cnt,
    MAX(e.amount) AS max_amount
FROM latest e
JOIN orders o ON e.order_id = o.id
GROUP BY o.status
ORDER BY status NULLS LAST
