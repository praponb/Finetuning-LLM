WITH latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        store_name,
        region,
        opened_date,
        sqft
    FROM stores
    QUALIFY ROW_NUMBER() OVER (PARTITION BY city ORDER BY opened_date ASC, id ASC) = 1
),
with_last AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.status
    FROM latest s
    JOIN LATERAL (
            SELECT c.status
            FROM orders c
            WHERE c.store_id = s.id
            ORDER BY c.shipping_fee DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        NTILE(10) OVER w_hist AS sqft_bucket,
        LEAD(sqft, 1, 0) OVER (PARTITION BY region ORDER BY sqft ASC, id ASC) AS next_sqft,
        ROW_NUMBER() OVER w_recent AS event_seq
    FROM with_last
    WHERE region = 'South'
    WINDOW w_hist AS (PARTITION BY region ORDER BY sqft DESC, id ASC), w_recent AS (PARTITION BY region, status ORDER BY sqft ASC, id ASC)
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY status ORDER BY opened_date DESC NULLS FIRST) AS dedup_rank
        FROM enriched
    ) x
    WHERE dedup_rank = 1
),
with_last_stores AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.last_restock,
        top1.inventory_quantity
    FROM deduped s
    JOIN LATERAL (
            SELECT c.last_restock, c.quantity AS inventory_quantity
            FROM inventory c
            WHERE c.store_id = s.id
            ORDER BY c.last_restock DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
wide AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            region,
            status,
            next_sqft
        FROM with_last_stores
    ) src
    PIVOT (
        MAX(next_sqft)
        FOR status IN ('COMPLETED', 'PENDING', 'CANCELLED')
    )
)
SELECT
    region,
    PENDING,
    CANCELLED
FROM wide;
