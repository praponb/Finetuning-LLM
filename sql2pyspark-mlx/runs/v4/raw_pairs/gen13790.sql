WITH latest AS (
    SELECT *, RANK() OVER (PARTITION BY order_id ORDER BY unit_price DESC NULLS LAST) AS rn
    FROM order_items
    QUALIFY rn = 1
),
scored AS (
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        rn,
        PERCENT_RANK() OVER (PARTITION BY order_id ORDER BY unit_price DESC NULLS LAST) AS unit_price_percent_rank,
        ROUND(unit_price / SUM(unit_price) OVER (PARTITION BY product_id), 4) AS unit_price_share,
        AVG(unit_price) OVER (PARTITION BY product_id ORDER BY quantity, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS unit_price_avg_2_prior,
        SUM(quantity) OVER (PARTITION BY order_id ORDER BY rn, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_quantity,
        ROUND(unit_price / SUM(unit_price) OVER (PARTITION BY product_id), 4) AS unit_price_share_2
    FROM latest
),
with_windows AS (
    SELECT
        *,
        LAG(unit_price_share, 1) OVER (PARTITION BY product_id, order_id ORDER BY unit_price_share, id) AS prev_unit_price_share,
        ROW_NUMBER() OVER w_recent AS seq_no
    FROM scored
    WINDOW w_recent AS (PARTITION BY order_id ORDER BY unit_price, id)
)
SELECT *
FROM (
    SELECT
        id,
        product_id,
        unit_price,
        prev_unit_price_share,
        unit_price_share_2,
        unit_price_percent_rank
    FROM with_windows
) src
UNPIVOT INCLUDE NULLS (
    value FOR kpi IN (unit_price, prev_unit_price_share, unit_price_share_2, unit_price_percent_rank)
)
