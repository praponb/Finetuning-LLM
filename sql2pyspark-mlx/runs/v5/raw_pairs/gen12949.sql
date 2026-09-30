WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        DATE_FORMAT(last_restock, 'MMM yyyy') last_restock_label,
        LAST_DAY(last_restock) last_restock_month_end
    FROM inventory
    WHERE last_restock >= DATE '2023-07-23'
      AND MONTH(last_restock) IN (2, 6, 8)
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM valid
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY last_restock_month_end DESC NULLS LAST, id) = 1
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_id,
        last_restock_label,
        MIN(quantity) OVER (PARTITION BY product_id, store_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_min_quantity,
        SUM(quantity) OVER (PARTITION BY product_id, store_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_sum_quantity,
        AVG(quantity) OVER (PARTITION BY product_id, store_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_avg_quantity,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY quantity, id) seq_no
    FROM deduped
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY store_id ORDER BY seq_no NULLS LAST) AS rn
        FROM scored
    ) t
    WHERE rn = 1
),
deduped_inventory AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY running_sum_quantity DESC, id) AS rank_in_group
    FROM ranked
    QUALIFY rank_in_group = 1
),
with_dims AS (
    -- Attach stores attributes
    SELECT
        a.*,
        d.region,
        d.opened_date
    FROM deduped_inventory a
    LEFT JOIN stores d
        ON a.store_id = d.id
),
grouped AS (
    -- Multi-level aggregation
    SELECT
        region,
        DATE_TRUNC('WEEK', opened_date) AS week_start,
        COUNT_IF(region = 'Central') AS central_events,
        APPROX_PERCENTILE(running_min_quantity, ARRAY(0.25, 0.5, 0.75)) AS running_min_quantity_quartiles,
        COUNT(*) AS event_count,
        PERCENT_RANK() OVER (PARTITION BY region ORDER BY COUNT(*) DESC) AS event_count_percent_rank
    FROM with_dims
    GROUP BY 1, 2
    HAVING COUNT_IF(region = 'Central') >= 1
)
SELECT
    region,
    week_start,
    central_events,
    running_min_quantity_quartiles,
    event_count_percent_rank,
    CASE WHEN region RLIKE '^(East|Central)$' THEN 'GROUP_B' WHEN region RLIKE '^(North|South)$' THEN 'EXTENDED' ELSE 'GROUP_A' END AS region_zone
FROM grouped;
