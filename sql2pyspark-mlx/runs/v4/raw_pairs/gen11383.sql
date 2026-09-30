WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_name,
        city,
        region,
        CONCAT_WS(',', store_name, region) store_name_region_key,
        UPPER(region) region_upper
    FROM stores
    WHERE region IS NOT NULL
),
with_last AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.order_date
    FROM filtered s
    JOIN LATERAL (
            SELECT c.order_date
            FROM orders c
            WHERE c.store_id = s.id
            ORDER BY c.shipping_fee DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
with_latest AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.quantity
    FROM with_last s
    JOIN LATERAL (
            SELECT c.quantity
            FROM inventory c
            WHERE c.store_id = s.id
            ORDER BY c.last_restock DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY city, region ORDER BY quantity NULLS FIRST) quantity_percent_rank,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY quantity, id) seq_no
    FROM with_latest
)
-- Aggregate with conditional metrics
SELECT
    region,
    APPROX_PERCENTILE(quantity_percent_rank, 0.9) AS quantity_percent_rank_p90,
    APPROX_PERCENTILE(quantity, 0.9) AS quantity_p90
FROM enriched
GROUP BY 1
