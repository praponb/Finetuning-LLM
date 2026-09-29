WITH base AS (
    SELECT
        id,
        store_id,
        order_date,
        status,
        channel,
        amount,
        COALESCE(channel, 'N/A') AS channel_filled,
        amount * discount AS amount_times_discount,
        COALESCE(channel, 'NONE') AS channel_filled_2
    FROM orders
    WHERE channel = 'mobile'
),
enriched AS (
    SELECT
        id,
        store_id,
        order_date,
        status,
        channel,
        amount,
        channel_filled,
        amount_times_discount,
        channel_filled_2,
        ROW_NUMBER() OVER (ORDER BY order_date DESC, id) AS row_seq,
        MIN(amount_times_discount) OVER (PARTITION BY store_id) AS store_id_min_amount_times_discount,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY amount DESC, id) AS row_seq_2
    FROM base
),
latest AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY order_date, id) AS rank_in_group
        FROM enriched
    ) ranked
    WHERE rank_in_group = 1
)
SELECT
    id,
    store_id,
    order_date,
    status,
    amount,
    amount_times_discount,
    channel_filled_2,
    store_id_min_amount_times_discount,
    rank_in_group
FROM latest
ORDER BY store_id NULLS LAST, order_date
