WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, INITCAP(TRIM(status)) AS status_title
    FROM shipments
    WHERE status IN ('IN_TRANSIT', 'RETURNED')
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        carrier,
        status,
        status_title,
        COUNT(cost) OVER (PARTITION BY order_id) AS order_id_count_cost,
        AVG(cost) OVER (PARTITION BY status ORDER BY shipped_date ASC, id ASC ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS cost_avg_2_prior,
        ROW_NUMBER() OVER (ORDER BY delivered_date ASC, id ASC) AS event_seq
    FROM cleaned
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MIN(order_id_count_cost) OVER (PARTITION BY carrier, status) AS carrier_min_order_id_count_cost,
        PERCENT_RANK() OVER (PARTITION BY carrier ORDER BY cost_avg_2_prior DESC NULLS LAST) AS cost_avg_2_prior_percent_rank
    FROM with_windows
    WHERE status IN ('RETURNED', 'DELIVERED')
),
with_windows_shipments AS (
    -- Ranking and gap features
    SELECT
        *,
        MIN(cost_avg_2_prior_percent_rank) OVER w_grp AS running_min_cost_avg_2_prior_percent_rank,
        COUNT(cost_avg_2_prior_percent_rank) OVER w_grp AS running_count_cost_avg_2_prior_percent_rank,
        SUM(cost_avg_2_prior_percent_rank) OVER w_grp AS running_sum_cost_avg_2_prior_percent_rank,
        AVG(cost_avg_2_prior_percent_rank) OVER w_grp AS running_avg_cost_avg_2_prior_percent_rank,
        SUM(order_id_count_cost) OVER w_part AS running_order_id_count_cost
    FROM enriched
    WINDOW w_grp AS (PARTITION BY carrier, status ORDER BY event_seq ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_part AS (PARTITION BY status, carrier ORDER BY cost_avg_2_prior ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    carrier,
    status,
    order_id_count_cost,
    cost_avg_2_prior,
    carrier_min_order_id_count_cost,
    running_count_cost_avg_2_prior_percent_rank,
    running_sum_cost_avg_2_prior_percent_rank,
    running_avg_cost_avg_2_prior_percent_rank
FROM with_windows_shipments
ORDER BY carrier DESC, id DESC
