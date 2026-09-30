WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LAST_DAY(order_date) AS order_date_month_end
    FROM orders
    WHERE channel IS NOT NULL
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(amount) OVER (PARTITION BY customer_id ORDER BY shipping_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_amount,
        PERCENT_RANK() OVER (PARTITION BY status ORDER BY amount DESC NULLS FIRST) AS amount_percent_rank,
        RANK() OVER (PARTITION BY status ORDER BY discount) AS discount_rank,
        LEAD(discount, 1) OVER (ORDER BY order_date, id) AS next_discount,
        CORR(shipping_fee, amount) OVER (PARTITION BY channel) AS corr_shipping_fee_amount
    FROM base
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, RANK() OVER (PARTITION BY store_id, customer_id ORDER BY shipping_fee) AS row_num
    FROM enriched
    QUALIFY row_num = 1
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, FLOOR(shipping_fee) AS shipping_fee_whole
    FROM deduped
    WHERE channel = 'store'
)
-- Ranking and gap features
SELECT
    id,
    customer_id,
    store_id,
    order_date,
    status,
    channel,
    amount,
    discount,
    shipping_fee,
    order_date_month_end,
    cumulative_amount,
    amount_percent_rank,
    discount_rank,
    next_discount,
    corr_shipping_fee_amount,
    row_num,
    shipping_fee_whole,
    STDDEV_SAMP(amount_percent_rank) OVER (PARTITION BY store_id, customer_id) AS store_id_stddev_samp_amount_percent_rank,
    NTILE(4) OVER (PARTITION BY customer_id ORDER BY corr_shipping_fee_amount DESC, id) AS corr_shipping_fee_amount_bucket,
    LAST_VALUE(next_discount, TRUE) OVER (PARTITION BY status ORDER BY row_num, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_next_discount
FROM filtered
