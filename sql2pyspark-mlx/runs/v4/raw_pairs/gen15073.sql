WITH cleaned AS (
    SELECT *, ADD_MONTHS(delivered_date, 3) delivered_date_plus_months
    FROM shipments
    WHERE order_id IS NOT NULL
),
enriched AS (
    SELECT
        src.*,
        d.channel,
        d.discount
    FROM cleaned src
    LEFT JOIN orders d
        ON src.order_id = d.id
),
metrics AS (
    SELECT
        *,
        LAG(discount, 1) OVER (PARTITION BY carrier, order_id ORDER BY delivered_date_plus_months, id) prev_discount,
        AVG(cost) OVER (PARTITION BY order_id ORDER BY cost, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) cost_avg_5,
        SUM(discount) OVER (PARTITION BY order_id ORDER BY shipped_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_discount,
        SUM(discount) OVER (PARTITION BY channel, status ORDER BY discount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_discount_2
    FROM enriched
),
deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY channel, carrier ORDER BY cost_avg_5, id) AS latest_rank
    FROM metrics
    QUALIFY latest_rank = 1
)
SELECT *
FROM (
    SELECT
        id,
        status,
        cost,
        discount,
        prev_discount
    FROM deduped
) src
UNPIVOT (
    metric_value FOR metric IN (cost AS COST, discount, prev_discount AS PREV_DISCOUNT_VAL)
);
