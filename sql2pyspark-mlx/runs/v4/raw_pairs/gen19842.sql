WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        FLOOR(amount) amount_whole,
        COALESCE(method, 'N/A') method_clean
    FROM payments
    WHERE paid_at > TO_DATE('2024-03-21')
      AND order_id IS NOT NULL
      AND method = 'cash'
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        method,
        amount,
        amount_whole,
        TO_DATE(paid_at) paid_at_date
    FROM valid
    WHERE paid_at > TIMESTAMP '2024-05-07 07:00:00'
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        order_id,
        method,
        amount,
        amount_whole,
        paid_at_date,
        AVG(amount_whole) OVER (PARTITION BY method ORDER BY amount_whole, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) amount_whole_avg_2,
        ROW_NUMBER() OVER (PARTITION BY method ORDER BY paid_at_date, id) event_seq,
        LAST_VALUE(method) IGNORE NULLS OVER (PARTITION BY method ORDER BY paid_at_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_method
    FROM base
),
enriched_payments AS (
    -- Attach orders attributes
    SELECT
        src.*,
        d.channel,
        d.shipping_fee,
        d.discount
    FROM enriched src
    LEFT JOIN orders d
        ON src.order_id = d.id
)
-- Grouped metrics
SELECT
    CASE WHEN shipping_fee >= 32 THEN 'PREMIUM' ELSE 'STANDARD' END AS shipping_fee_tier,
    COUNT(CASE WHEN channel = 'web' THEN 1 END) AS web_count,
    COUNT(*) AS event_count,
    MAX(amount) FILTER (WHERE channel = 'partner') AS partner_max_amount,
    SORT_ARRAY(COLLECT_SET(method)) AS method_list,
    ROUND(SUM(amount_whole_avg_2) / NULLIF(COUNT(CASE WHEN method = 'bank_transfer' THEN 1 END), 0), 2) AS amount_whole_avg_2_per_bank_transfer,
    AVG(shipping_fee) FILTER (WHERE channel = 'web') AS web_avg_shipping_fee
FROM enriched_payments
GROUP BY 1
ORDER BY amount_whole_avg_2_per_bank_transfer DESC;
