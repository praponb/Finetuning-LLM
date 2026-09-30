WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        COALESCE(cost, 0) cost_filled,
        LAST_DAY(delivered_date) delivered_date_month_end,
        COALESCE(carrier, 'N/A') resolved_carrier
    FROM shipments
    WHERE shipped_date > TO_DATE('2023-04-29')
      AND carrier IS DISTINCT FROM 'DHL'
),
joined AS (
    -- Attach orders attributes
    SELECT
        a.*,
        d.channel,
        d.amount,
        d.discount
    FROM valid a
    LEFT JOIN orders d
        ON a.order_id = d.id
    WHERE d.customer_id IN (18, 24, 38)
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct
    FROM joined e
    INNER JOIN promotions pr
        ON e.channel <=> pr.channel
       AND e.shipped_date BETWEEN pr.starts_on AND pr.ends_on
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        carrier,
        delivered_date,
        status,
        delivered_date_month_end,
        amount,
        discount,
        promo_id,
        promo_code,
        discount_pct,
        MIN(cost) OVER (PARTITION BY channel, promo_code ORDER BY delivered_date, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_min_cost,
        AVG(cost) OVER (PARTITION BY channel, promo_code ORDER BY delivered_date, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_avg_cost,
        MAX(cost) OVER (PARTITION BY channel, promo_code ORDER BY delivered_date, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_max_cost,
        SUM(cost) OVER (PARTITION BY channel, promo_code ORDER BY delivered_date, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_sum_cost,
        AVG(discount) OVER (PARTITION BY channel ORDER BY discount, id, promo_id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) discount_avg_6_prior
    FROM with_window
)
SELECT
    id,
    carrier,
    delivered_date,
    status,
    delivered_date_month_end,
    discount,
    promo_id,
    promo_code,
    discount_pct,
    CASE WHEN status RLIKE '^(LOST|IN_TRANSIT)$' THEN 'GROUP_A' WHEN status RLIKE '^(DELIVERED|RETURNED)$' THEN 'PRIORITY' ELSE 'CORE' END AS status_zone
FROM metrics
ORDER BY status_zone DESC NULLS FIRST;
