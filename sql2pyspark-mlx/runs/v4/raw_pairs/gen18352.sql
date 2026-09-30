WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN cost >= 100 THEN 'High' WHEN cost >= 29 THEN 'Medium' ELSE 'Low' END AS cost_level,
        COALESCE(carrier, 'NONE') AS carrier_clean
    FROM shipments
    WHERE carrier IS NOT NULL
      AND status = 'IN_TRANSIT'
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CEIL(cost) AS cost_whole,
        COALESCE(carrier, 'NONE') AS carrier_clean_2
    FROM base
    WHERE shipped_date > TO_DATE('2024-03-20')
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        cost_level,
        carrier_clean,
        cost_whole,
        carrier_clean_2,
        LAST_VALUE(cost_whole) IGNORE NULLS OVER (ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_cost_whole,
        NTILE(5) OVER (PARTITION BY status, order_id ORDER BY cost_whole DESC, id) AS cost_whole_bucket,
        LAG(cost_whole, 1) OVER (ORDER BY delivered_date, id) AS prev_cost_whole,
        AVG(cost) OVER (PARTITION BY carrier ORDER BY cost_whole, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS cost_avg_5_prior,
        AVG(cost_whole) OVER (PARTITION BY order_id ORDER BY cost_whole, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS cost_whole_avg_3_prior
    FROM valid
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        cost_level,
        carrier_clean,
        cost_whole,
        carrier_clean_2,
        last_known_cost_whole,
        cost_whole_bucket,
        prev_cost_whole,
        cost_avg_5_prior,
        cost_whole_avg_3_prior,
        RANK() OVER (PARTITION BY order_id, status ORDER BY cost_whole_avg_3_prior DESC NULLS FIRST) AS cost_whole_avg_3_prior_rank,
        AVG(cost_whole_avg_3_prior) OVER (PARTITION BY order_id ORDER BY cost_whole_avg_3_prior, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS cost_whole_avg_3_prior_avg_6_prior
    FROM with_windows
),
scored_shipments AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(cost_whole) OVER (PARTITION BY order_id ORDER BY delivered_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_cost_whole,
        SUM(cost_whole_avg_3_prior_avg_6_prior) OVER w_ordered AS running_cost_whole_avg_3_prior_avg_6_prior,
        ROUND(cost / SUM(cost) OVER (PARTITION BY carrier), 4) AS cost_share
    FROM scored
    WINDOW w_ordered AS (PARTITION BY carrier, status ORDER BY cost_whole, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        e.*,
        o.order_date,
        o.channel
    FROM scored_shipments e
    LEFT JOIN orders o
        ON e.order_id = o.id
)
SELECT
    id,
    order_id,
    delivered_date,
    cost,
    status,
    carrier_clean,
    cost_whole,
    carrier_clean_2,
    last_known_cost_whole,
    CASE WHEN status RLIKE '^(RETURNED|IN_TRANSIT)$' THEN 'EXTENDED' WHEN status RLIKE '^(DELIVERED|LOST)$' THEN 'PRIORITY' ELSE 'OTHER_ZONE' END AS status_zone
FROM with_dims
ORDER BY order_id DESC, status_zone;
