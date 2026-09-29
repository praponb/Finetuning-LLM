WITH remaining_keys AS (
    -- Set difference on keys
    SELECT status, channel
    FROM orders
    EXCEPT
    SELECT status, channel
    FROM orders
    WHERE YEAR(order_date) = 2023
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM orders s
    INNER JOIN remaining_keys k ON s.status = k.status AND s.channel = k.channel
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        CORR(amount, shipping_fee) OVER (PARTITION BY status, channel) AS corr_amount_shipping_fee,
        CORR(shipping_fee, amount) OVER (PARTITION BY status) AS corr_shipping_fee_amount,
        PERCENT_RANK() OVER (ORDER BY shipping_fee DESC NULLS FIRST) AS shipping_fee_percent_rank
    FROM kept_rows
    WHERE channel = 'store'
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        corr_shipping_fee_amount / NULLIF(shipping_fee_percent_rank, 0) AS corr_shipping_fee_amount_per_shipping_fee_percent_rank,
        COALESCE(channel, 'OTHER') AS channel_clean
    FROM scored
    WHERE shipping_fee BETWEEN 11 AND 12
),
matched_keys AS (
    -- Set difference on keys
    SELECT channel
    FROM filtered
    EXCEPT DISTINCT
    SELECT channel
    FROM orders
    WHERE discount >= 0.34
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    INNER JOIN matched_keys k ON s.channel = k.channel
)
SELECT
    s.id,
    s.customer_id,
    s.store_id,
    s.status,
    s.channel,
    s.amount,
    s.discount,
    s.shipping_fee,
    s.corr_amount_shipping_fee,
    (SELECT SUM(s2.units) FROM store_sales s2 WHERE s2.store_id = s.store_id) AS sum_store_sale_units
FROM verified s
ORDER BY corr_amount_shipping_fee DESC NULLS FIRST
