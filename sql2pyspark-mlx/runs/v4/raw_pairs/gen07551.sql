WITH filtered AS (
    SELECT
        *,
        QUARTER(paid_at) AS paid_at_quarter,
        CAST(paid_at AS DATE) AS paid_at_day,
        COALESCE(method, 'OTHER') AS method_filled
    FROM payments
    WHERE order_id IS NOT NULL
),
scored AS (
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        paid_at_quarter,
        paid_at_day,
        method_filled,
        COUNT(paid_at_quarter) OVER (PARTITION BY method) AS method_count_paid_at_quarter,
        FIRST_VALUE(paid_at_quarter) IGNORE NULLS OVER (PARTITION BY order_id ORDER BY paid_at_day, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_paid_at_quarter,
        SUM(paid_at_quarter) OVER (PARTITION BY method ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS paid_at_quarter_sum_7d,
        AVG(paid_at_quarter) OVER (ORDER BY paid_at, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS paid_at_quarter_avg_3
    FROM filtered
),
joined AS (
    SELECT
        src.*,
        o.order_date,
        o.status AS order_status
    FROM scored src
    INNER JOIN orders o
        ON src.order_id = o.id
)
SELECT
    DATE_TRUNC('WEEK', order_date) AS week_start,
    CASE WHEN first_known_paid_at_quarter >= 80 THEN 'LARGE' ELSE 'SMALL' END AS first_known_paid_at_quarter_tier,
    SUM(CASE WHEN order_status = 'RETURNED' THEN amount ELSE 0 END) AS returned_amount,
    AVG(method_count_paid_at_quarter) AS avg_method_count_paid_at_quarter,
    ROUND(AVG(paid_at_quarter_sum_7d), 2) AS avg_paid_at_quarter_sum_7d
FROM joined
GROUP BY GROUPING SETS ((DATE_TRUNC('WEEK', order_date), CASE WHEN first_known_paid_at_quarter >= 80 THEN 'LARGE' ELSE 'SMALL' END), (DATE_TRUNC('WEEK', order_date)))
ORDER BY avg_paid_at_quarter_sum_7d DESC NULLS LAST
