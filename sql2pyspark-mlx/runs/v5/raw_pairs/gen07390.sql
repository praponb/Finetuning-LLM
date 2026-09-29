WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(amount * 100, 2) AS amount_scaled,
        ADD_MONTHS(order_date, 3) AS order_date_plus_months,
        COALESCE(channel, 'UNKNOWN') AS channel_clean
    FROM orders
    WHERE customer_id IS NOT NULL
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM base s
    WHERE EXISTS (
            SELECT 1
            FROM payments x
            WHERE x.order_id = s.id
              AND x.paid_at <= s.order_date_plus_months
        )
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(shipping_fee) IGNORE NULLS OVER (PARTITION BY status ORDER BY amount ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_shipping_fee,
        SUM(shipping_fee) OVER (PARTITION BY store_id ORDER BY order_date_plus_months ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_shipping_fee
    FROM kept
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY order_date_plus_months ASC, id ASC) AS latest_rank
    FROM scored
    QUALIFY latest_rank = 1
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY channel, customer_id ORDER BY first_known_shipping_fee DESC NULLS LAST, id ASC) AS latest_rank_2
    FROM deduped
    QUALIFY latest_rank_2 = 1
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        status,
        discount,
        amount_scaled,
        amount,
        first_known_shipping_fee
    FROM latest
) src
UNPIVOT INCLUDE NULLS (
    val FOR metric_name IN (discount, amount_scaled, amount, first_known_shipping_fee)
);
