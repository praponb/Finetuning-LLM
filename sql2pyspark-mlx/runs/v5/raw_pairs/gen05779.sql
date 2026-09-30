WITH metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(discount) OVER (PARTITION BY channel ORDER BY discount, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS discount_avg_2,
        CORR(discount, shipping_fee) OVER (PARTITION BY status) AS corr_discount_shipping_fee,
        FIRST_VALUE(amount) IGNORE NULLS OVER w_grp AS first_known_amount,
        SUM(amount) OVER (PARTITION BY customer_id ORDER BY order_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_amount,
        SUM(discount) OVER (ORDER BY shipping_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_discount
    FROM orders
    WINDOW w_grp AS (PARTITION BY store_id ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Multi-level aggregation
SELECT
    CASE WHEN discount >= 0.43 THEN 'LARGE' ELSE 'SMALL' END AS discount_tier,
    store_id,
    COUNT_IF(status = 'SHIPPED') AS shipped_events,
    SUM(CASE WHEN channel = 'partner' THEN corr_discount_shipping_fee ELSE 0 END) AS partner_corr_discount_shipping_fee,
    SUM(running_discount) AS total_running_discount,
    COUNT(*) FILTER (WHERE status = 'SHIPPED') AS shipped_rows,
    SUM(discount_avg_2) AS total_discount_avg_2
FROM metrics
GROUP BY CASE WHEN discount >= 0.43 THEN 'LARGE' ELSE 'SMALL' END, store_id
