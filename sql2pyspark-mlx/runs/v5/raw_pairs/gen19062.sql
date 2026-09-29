WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(cost, 1) AS cost_rounded,
        SUBSTRING(carrier, 1, 3) AS carrier_prefix
    FROM shipments
    WHERE carrier IS NOT NULL
      AND carrier = 'UPS'
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY carrier, order_id ORDER BY cost DESC NULLS FIRST) AS cost_cume_dist,
        AVG(cost_rounded) OVER (ORDER BY cost, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS cost_rounded_avg_2_prior
    FROM filtered
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        cost_rounded_avg_2_prior + cost AS cost_rounded_avg_2_prior_plus_cost,
        COALESCE(carrier, 'NONE') AS carrier_filled
    FROM with_windows
    WHERE carrier IS NOT NULL
      AND status NOT IN ('RETURNED', 'IN_TRANSIT', 'DELIVERED')
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(cost, 1) OVER (ORDER BY shipped_date, id) AS prev_cost,
        SUM(cost_rounded_avg_2_prior) OVER (PARTITION BY status ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_cost_rounded_avg_2_prior,
        CUME_DIST() OVER (PARTITION BY order_id, status ORDER BY cost_rounded DESC) AS cost_rounded_cume_dist,
        FIRST_VALUE(cost_rounded_avg_2_prior) IGNORE NULLS OVER (PARTITION BY order_id, carrier ORDER BY cost_rounded, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_cost_rounded_avg_2_prior
    FROM cleaned
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        shipped_date,
        delivered_date,
        cost,
        cost_rounded,
        cost_cume_dist,
        cost_rounded_avg_2_prior,
        cost_rounded_avg_2_prior_plus_cost,
        running_cost_rounded_avg_2_prior,
        first_known_cost_rounded_avg_2_prior,
        NTILE(5) OVER (PARTITION BY status, carrier ORDER BY cost_rounded, id) AS cost_rounded_bucket,
        COUNT(cost_cume_dist) OVER (PARTITION BY carrier) AS carrier_count_cost_cume_dist,
        SUM(cost_rounded) OVER (PARTITION BY order_id, carrier ORDER BY delivered_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_cost_rounded,
        ROW_NUMBER() OVER (ORDER BY cost, id) AS event_seq,
        ROUND(cost / SUM(cost) OVER (PARTITION BY order_id), 4) AS cost_share
    FROM enriched
    WHERE carrier = 'Kerry'
),
cleaned_shipments AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, FLOOR(cost_rounded) AS cost_rounded_whole
    FROM metrics
    WHERE event_seq = 7
)
SELECT
    id,
    shipped_date,
    delivered_date,
    cost,
    cost_rounded,
    cost_rounded_avg_2_prior,
    cost_rounded_avg_2_prior_plus_cost,
    first_known_cost_rounded_avg_2_prior,
    cost_rounded_bucket
FROM cleaned_shipments
ORDER BY cost_rounded_avg_2_prior_plus_cost DESC;
