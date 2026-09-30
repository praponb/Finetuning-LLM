WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        method,
        amount,
        status,
        HOUR(paid_at) AS paid_at_hour
    FROM payments
    WHERE order_id IS NOT NULL
      AND status = 'REFUNDED'
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN method IN ('paypal', 'bank_transfer') THEN 'Group A' ELSE 'Other' END AS method_group,
        COALESCE(method, 'N/A') AS resolved_method
    FROM cleaned
    WHERE paid_at_hour BETWEEN 53 AND 71
),
filtered_payments AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        UPPER(status) AS status_upper,
        ROUND(paid_at_hour, 2) AS paid_at_hour_rounded
    FROM filtered
    WHERE method IS DISTINCT FROM 'promptpay'
      AND method = 'cash'
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(amount) OVER (PARTITION BY method ORDER BY amount ASC, id ASC ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS amount_avg_6_prior,
        PERCENT_RANK() OVER (PARTITION BY method ORDER BY paid_at_hour_rounded DESC NULLS LAST) AS paid_at_hour_rounded_percent_rank
    FROM filtered_payments
    WHERE amount IS NOT NULL
)
SELECT
    id,
    amount,
    status,
    paid_at_hour,
    resolved_method,
    paid_at_hour_rounded,
    amount_avg_6_prior
FROM enriched;
