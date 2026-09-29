WITH valid AS (
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        cost,
        status,
        ADD_MONTHS(shipped_date, 1) AS shipped_date_plus_months
    FROM shipments
    WHERE delivered_date > DATE '2023-09-09'
      AND shipped_date BETWEEN '2024-06-01' AND '2025-09-23'
),
valid_shipments AS (
    SELECT
        *,
        LAST_DAY(shipped_date_plus_months) AS shipped_date_plus_months_month_end,
        COALESCE(carrier, 'DIRECT') AS resolved_carrier
    FROM valid
    WHERE shipped_date > DATE '2023-05-12'
      AND order_id IS NOT NULL
),
enriched AS (
    SELECT
        id,
        order_id,
        carrier,
        cost,
        shipped_date_plus_months,
        STDDEV_SAMP(cost) OVER (PARTITION BY carrier) AS carrier_stddev_samp_cost,
        PERCENT_RANK() OVER (PARTITION BY carrier ORDER BY cost DESC NULLS LAST) AS cost_percent_rank,
        LAG(cost, 1) OVER (ORDER BY shipped_date_plus_months_month_end, id) AS prev_cost
    FROM valid_shipments
    WHERE shipped_date_plus_months_month_end < '2025-08-01'
),
latest AS (
    SELECT *
    FROM enriched
    QUALIFY RANK() OVER (PARTITION BY order_id, carrier ORDER BY cost DESC) = 1
),
enriched_shipments AS (
    SELECT
        s.*,
        b.amount,
        b.channel,
        b.discount
    FROM latest s
    LEFT JOIN orders b
        ON s.order_id = b.id
),
by_type AS (
    SELECT *
    FROM (
        SELECT
            order_id,
            carrier,
            cost
        FROM enriched_shipments
    ) src
    PIVOT (
        AVG(cost)
        FOR carrier IN ('DHL', 'UPS')
    )
)
SELECT order_id
FROM by_type;
