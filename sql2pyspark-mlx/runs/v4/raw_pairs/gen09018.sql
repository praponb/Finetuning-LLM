WITH base AS (
    -- Basic cleansing and defaults
    SELECT *, CEIL(quantity) quantity_whole
    FROM order_items
    WHERE order_id IN (7, 31)
),
joined AS (
    -- Attach products attributes
    SELECT
        e.*,
        p.launch_date,
        p.product_name
    FROM base e
    INNER JOIN products p
        ON e.product_id = p.id
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY launch_date DESC, id) AS dedup_rank
        FROM joined
    ) ranked
    WHERE dedup_rank = 1
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(quantity_whole) OVER (PARTITION BY product_id ORDER BY launch_date, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) quantity_whole_avg_3_prior,
        SUM(quantity) OVER (PARTITION BY product_id, order_id ORDER BY dedup_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_quantity,
        AVG(unit_price) OVER (PARTITION BY order_id, product_id ORDER BY unit_price, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) unit_price_avg_3_prior,
        NTILE(3) OVER (PARTITION BY order_id ORDER BY quantity_whole, id) quantity_whole_bucket
    FROM deduped
),
remaining_keys AS (
    -- Set difference on keys
    SELECT product_id, order_id
    FROM enriched
    EXCEPT
    SELECT product_id, order_id
    FROM order_items
    WHERE unit_price >= 370
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    INNER JOIN remaining_keys k ON s.product_id = k.product_id AND s.order_id = k.order_id
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        CUME_DIST() OVER w_running quantity_whole_cume_dist,
        ROUND(unit_price_avg_3_prior / SUM(unit_price_avg_3_prior) OVER (PARTITION BY order_id), 4) unit_price_avg_3_prior_share,
        PERCENT_RANK() OVER (ORDER BY quantity_whole_bucket DESC NULLS LAST) quantity_whole_bucket_percent_rank
    FROM reconciled
    WINDOW w_running AS (ORDER BY quantity_whole DESC NULLS FIRST)
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN quantity_whole >= 79 THEN 'PREMIUM' ELSE 'STANDARD' END AS quantity_whole_tier,
        order_id,
        CASE WHEN quantity_whole_bucket_percent_rank >= 0.67 THEN 'LARGE' ELSE 'SMALL' END AS quantity_whole_bucket_percent_rank_tier,
        PERCENTILE_APPROX(quantity_whole_cume_dist, ARRAY(0.25, 0.5, 0.75)) AS quantity_whole_cume_dist_quartiles,
        BIT_AND(quantity) AS quantity_bit_and,
        PERCENTILE_APPROX(quantity, ARRAY(0.25, 0.5, 0.75)) AS quantity_quartiles
    FROM with_windows
    GROUP BY quantity_whole_tier, order_id, quantity_whole_bucket_percent_rank_tier
)
SELECT
    s.quantity_whole_tier,
    s.order_id,
    s.quantity_whole_bucket_percent_rank_tier,
    s.quantity_bit_and,
    s.quantity_quartiles,
    (SELECT AVG(t2.cost) FROM shipments t2 WHERE t2.order_id = s.order_id) AS avg_shipment_cost
FROM grouped s
ORDER BY quantity_whole_tier DESC NULLS LAST;
