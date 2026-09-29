WITH enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        carrier,
        delivered_date,
        SUM(cost) OVER (PARTITION BY carrier ORDER BY shipped_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_cost,
        ROUND(cost / SUM(cost) OVER (PARTITION BY order_id), 4) AS cost_share
    FROM shipments
    WHERE cost >= 83
),
enriched_shipments AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        carrier,
        cost_share,
        AVG(cumulative_cost) OVER w_ordered AS running_avg_cumulative_cost,
        MIN(cumulative_cost) OVER w_ordered AS running_min_cumulative_cost,
        SUM(cumulative_cost) OVER w_ordered AS running_sum_cumulative_cost
    FROM enriched
    WINDOW w_ordered AS (PARTITION BY carrier ORDER BY cost_share, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ROUND(cost_share * 1.1, 2) AS cost_share_scaled
    FROM enriched_shipments
    WHERE carrier IN ('Flash', 'UPS')
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        carrier,
        running_avg_cumulative_cost,
        running_min_cumulative_cost,
        running_sum_cumulative_cost,
        cost_share_scaled,
        cost_share * cost_share_scaled AS cost_share_times_cost_share_scaled,
        COALESCE(carrier, 'NONE') AS resolved_carrier
    FROM valid
    WHERE carrier NOT IN ('Flash', 'FedEx', 'UPS')
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        ROUND(running_min_cumulative_cost / SUM(running_min_cumulative_cost) OVER (PARTITION BY carrier), 4) AS running_min_cumulative_cost_share,
        SUM(running_sum_cumulative_cost) OVER (PARTITION BY carrier ORDER BY cost_share_times_cost_share_scaled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_running_sum_cumulative_cost,
        PERCENT_RANK() OVER (PARTITION BY carrier ORDER BY running_min_cumulative_cost DESC) AS running_min_cumulative_cost_percent_rank,
        LEAD(running_sum_cumulative_cost, 2) OVER (PARTITION BY carrier ORDER BY cost_share_times_cost_share_scaled, id) AS next_running_sum_cumulative_cost,
        STDDEV_SAMP(cost_share_times_cost_share_scaled) OVER (PARTITION BY carrier) AS carrier_stddev_samp_cost_share_times_cost_share_scaled
    FROM cleaned
)
-- Aggregate with conditional metrics
SELECT
    carrier,
    CASE WHEN carrier_stddev_samp_cost_share_times_cost_share_scaled >= 78 THEN 'LARGE' ELSE 'SMALL' END AS carrier_stddev_samp_cost_share_times_cost_share_scaled_tier,
    SIZE(COLLECT_SET(carrier)) AS num_carriers,
    SUM(CASE WHEN carrier = 'DHL' THEN cumulative_running_sum_cumulative_cost ELSE 0 END) AS dhl_cumulative_running_sum_cumulative_cost,
    ROUND(AVG(cost_share_times_cost_share_scaled), 2) AS avg_cost_share_times_cost_share_scaled
FROM with_windows
GROUP BY ROLLUP (carrier, CASE WHEN carrier_stddev_samp_cost_share_times_cost_share_scaled >= 78 THEN 'LARGE' ELSE 'SMALL' END)
HAVING avg_cost_share_times_cost_share_scaled > 0
ORDER BY dhl_cumulative_running_sum_cumulative_cost
