WITH deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id, product_id ORDER BY quantity NULLS LAST, id) AS rank_in_group
    FROM order_items
    QUALIFY rank_in_group = 1
),
enriched AS (
    SELECT e.*, b.launch_date
    FROM deduped e
    LEFT JOIN products b
        ON e.product_id = b.id
),
enriched_order_items AS (
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY order_id ORDER BY quantity DESC NULLS FIRST) AS quantity_cume_dist,
        STDDEV_SAMP(unit_price) OVER w_grp AS order_id_stddev_samp_unit_price,
        AVG(quantity) OVER w_grp_2 AS quantity_avg_2,
        NTILE(5) OVER w_ordered AS unit_price_bucket
    FROM enriched
    WINDOW w_grp AS (PARTITION BY order_id), w_grp_2 AS (PARTITION BY order_id, product_id ORDER BY quantity, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), w_ordered AS (PARTITION BY product_id, order_id ORDER BY unit_price, id)
),
deduped_order_items AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY unit_price_bucket DESC NULLS LAST, id) AS row_num
        FROM enriched_order_items
    ) t
    WHERE row_num = 1
),
enriched_order_items_2 AS (
    SELECT
        a.*,
        p.cost,
        p.launch_date AS product_launch_date
    FROM deduped_order_items a
    LEFT JOIN products p
        ON a.product_id = p.id
),
deduped_order_items_2 AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY cost DESC NULLS FIRST, id) AS latest_rank
        FROM enriched_order_items_2
    ) ranked
    WHERE latest_rank = 1
),
metrics AS (
    SELECT
        *,
        LAG(order_id_stddev_samp_unit_price, 1) OVER w_grp AS prev_order_id_stddev_samp_unit_price,
        SUM(cost) OVER (PARTITION BY product_id, order_id ORDER BY rank_in_group, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_cost,
        CORR(quantity, unit_price) OVER w_running AS corr_quantity_unit_price
    FROM deduped_order_items_2
    WINDOW w_grp AS (PARTITION BY order_id ORDER BY quantity, id), w_running AS (PARTITION BY product_id)
),
daily AS (
    SELECT
        order_id,
        CASE WHEN quantity >= 8 THEN 'HIGH' ELSE 'LOW' END AS quantity_tier,
        ROUND(AVG(order_id_stddev_samp_unit_price), 2) AS avg_order_id_stddev_samp_unit_price,
        COUNT(*) AS num_rows,
        GROUPING_ID() AS grouping_id
    FROM metrics
    GROUP BY GROUPING SETS ((order_id, CASE WHEN quantity >= 8 THEN 'HIGH' ELSE 'LOW' END), (order_id))
)
SELECT quantity_tier, num_rows
FROM daily;
