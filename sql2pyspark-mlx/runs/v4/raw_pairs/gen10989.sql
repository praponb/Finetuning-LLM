WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        amount + shipping_fee amount_with_shipping,
        shipping_fee * discount shipping_fee_times_discount
    FROM orders
    WHERE channel IS NULL
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        TRIM(channel) clean_channel,
        COALESCE(channel, 'N/A') channel_clean
    FROM filtered
    WHERE channel IS NOT NULL
      AND customer_id IS NOT NULL
),
with_windows AS (
    -- Window metrics over several different partitions
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
        amount_with_shipping,
        shipping_fee_times_discount,
        clean_channel,
        channel_clean,
        ROW_NUMBER() OVER (PARTITION BY channel, customer_id ORDER BY order_date DESC, id ASC) seq_no,
        PERCENT_RANK() OVER (ORDER BY shipping_fee DESC) shipping_fee_percent_rank
    FROM cleaned
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_windows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM payments ch
            WHERE ch.order_id = s.id
              AND ch.paid_at <= s.order_date
        )
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        customer_id,
        amount,
        shipping_fee,
        shipping_fee_percent_rank,
        amount_with_shipping
    FROM kept
) src
UNPIVOT (
    val FOR metric IN (amount, shipping_fee, shipping_fee_percent_rank, amount_with_shipping)
)
