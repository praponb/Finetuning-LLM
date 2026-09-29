WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        method,
        amount,
        status,
        INITCAP(TRIM(status)) status_title,
        COALESCE(method, 'OTHER') method_filled
    FROM payments
    WHERE paid_at >= TIMESTAMP '2023-11-15 14:00:00'
      AND order_id IS NOT NULL
),
enriched AS (
    -- Attach orders attributes
    SELECT
        s.*,
        p.channel,
        p.status AS order_status
    FROM base s
    LEFT JOIN orders p
        ON s.order_id = p.id
),
joined AS (
    -- Attach orders attributes
    SELECT
        s.*,
        p.amount AS order_amount,
        p.discount,
        p.status AS order_status_2
    FROM enriched s
    INNER JOIN orders p
        ON s.order_id = p.id
    WHERE p.order_date BETWEEN '2023-10-22' AND '2025-10-01'
),
enriched_payments AS (
    -- Ranking and gap features
    SELECT
        id,
        method,
        amount,
        status_title,
        method_filled,
        channel,
        MIN(discount) OVER w_ordered running_min_discount,
        MAX(discount) OVER w_ordered running_max_discount,
        COUNT(discount) OVER w_ordered running_count_discount,
        SUM(discount) OVER w_ordered running_sum_discount,
        AVG(discount) OVER (PARTITION BY method) method_avg_discount
    FROM joined
    WINDOW w_ordered AS (PARTITION BY method ORDER BY order_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        method,
        method_filled,
        running_min_discount,
        running_max_discount,
        running_count_discount,
        running_sum_discount,
        method_avg_discount,
        SUM(running_sum_discount) OVER w_part running_sum_running_sum_discount,
        MIN(running_sum_discount) OVER w_part running_min_running_sum_discount,
        AVG(running_sum_discount) OVER w_part running_avg_running_sum_discount,
        COUNT(running_sum_discount) OVER w_part running_count_running_sum_discount,
        ROW_NUMBER() OVER (PARTITION BY channel ORDER BY running_count_discount, id) event_seq
    FROM enriched_payments
    WINDOW w_part AS (PARTITION BY channel ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        method_filled,
        running_min_discount,
        running_sum_discount,
        method_avg_discount,
        running_min_running_sum_discount,
        running_avg_running_sum_discount,
        running_count_running_sum_discount,
        event_seq,
        SUM(running_min_discount) OVER (PARTITION BY method ORDER BY event_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_running_min_discount,
        AVG(method_avg_discount) OVER (ORDER BY running_max_discount, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) method_avg_discount_avg_5_prior
    FROM scored
)
SELECT
    id,
    method_filled,
    running_min_discount,
    running_sum_discount,
    method_avg_discount,
    running_count_running_sum_discount,
    event_seq,
    cumulative_running_min_discount,
    method_avg_discount_avg_5_prior
FROM with_windows
ORDER BY event_seq DESC, id DESC;
