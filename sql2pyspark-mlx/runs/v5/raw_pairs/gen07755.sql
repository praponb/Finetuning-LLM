WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY shipped_date DESC, id) AS dedup_rank
    FROM shipments
    QUALIFY dedup_rank = 1
),
filtered AS (
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        LOWER(status) AS status_lower
    FROM ranked
    WHERE delivered_date > DATE '2023-05-04'
      AND carrier IS DISTINCT FROM 'Kerry'
),
metrics AS (
    SELECT
        *,
        SUM(cost) OVER (PARTITION BY status ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_cost,
        STDDEV_SAMP(cost) OVER (PARTITION BY carrier) AS carrier_stddev_samp_cost,
        AVG(cost) OVER (PARTITION BY status, carrier) AS status_avg_cost
    FROM filtered
    WHERE shipped_date < '2024-07-01'
),
enriched AS (
    SELECT
        *,
        SUM(cumulative_cost) OVER (PARTITION BY status ORDER BY status_avg_cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_cumulative_cost,
        FIRST_VALUE(carrier_stddev_samp_cost) IGNORE NULLS OVER (PARTITION BY carrier ORDER BY delivered_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_carrier_stddev_samp_cost,
        LAG(cumulative_cost, 1) OVER (PARTITION BY status ORDER BY cumulative_cost, id) AS prev_cumulative_cost,
        MIN(cost) OVER (PARTITION BY carrier) AS carrier_min_cost
    FROM metrics
),
latest AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY cumulative_cost DESC, id) AS row_num
        FROM enriched
    ) ranked
    WHERE row_num = 1
),
deduped AS (
    SELECT
        id,
        delivered_date,
        cost,
        status,
        cumulative_cost,
        carrier_stddev_samp_cost,
        status_avg_cost,
        cumulative_cumulative_cost,
        first_known_carrier_stddev_samp_cost,
        carrier_min_cost,
        row_num
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id, carrier ORDER BY first_known_carrier_stddev_samp_cost DESC NULLS FIRST, id) AS rank_in_group
        FROM latest
    ) x
    WHERE rank_in_group = 1
),
cleaned AS (
    SELECT
        id,
        delivered_date,
        status,
        carrier_stddev_samp_cost,
        status_avg_cost,
        cumulative_cumulative_cost,
        first_known_carrier_stddev_samp_cost,
        carrier_min_cost,
        row_num,
        ADD_MONTHS(delivered_date, 12) AS delivered_date_plus_months
    FROM deduped
    WHERE delivered_date >= TO_DATE('2023-03-28')
)
SELECT
    id,
    delivered_date,
    status,
    carrier_stddev_samp_cost,
    status_avg_cost,
    cumulative_cumulative_cost,
    delivered_date_plus_months,
    CASE WHEN status RLIKE '^(DELIVERED|RETURNED)$' THEN 'PRIORITY' ELSE 'GROUP_B' END AS status_zone
FROM cleaned
ORDER BY cumulative_cumulative_cost NULLS FIRST;
