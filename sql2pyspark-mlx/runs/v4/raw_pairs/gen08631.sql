WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN sqft >= 13104 THEN 'High' WHEN sqft >= 4855 THEN 'Medium' ELSE 'Low' END sqft_band,
        CASE WHEN city IN ('Berlin', 'London') THEN 'Tier 1' ELSE 'Other' END city_group
    FROM stores
    WHERE opened_date > TO_DATE('2023-11-08')
      AND opened_date BETWEEN '2023-03-01' AND '2025-08-01'
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(sqft, 2) OVER w_recent prev_sqft,
        ROW_NUMBER() OVER w_hist event_seq
    FROM base
    WINDOW w_recent AS (PARTITION BY region ORDER BY opened_date, id), w_hist AS (PARTITION BY region ORDER BY sqft DESC, id)
),
with_latest AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.quantity,
        top1.last_restock
    FROM enriched s
    JOIN LATERAL (
            SELECT c.quantity, c.last_restock
            FROM inventory c
            WHERE c.store_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        sqft_band,
        city_group,
        prev_sqft,
        event_seq,
        quantity,
        last_restock,
        NTILE(4) OVER (PARTITION BY region, city ORDER BY event_seq DESC, id) event_seq_bucket,
        SUM(prev_sqft) OVER (PARTITION BY city) city_sum_prev_sqft
    FROM with_latest
),
latest_match AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.revenue
    FROM scored s
    JOIN LATERAL (
            SELECT c.revenue
            FROM store_sales c
            WHERE c.store_id = s.id
            ORDER BY c.sale_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM latest_match s
    WHERE EXISTS (
            SELECT 1
            FROM store_sales x
            WHERE x.store_id = s.id
              AND x.sale_date <= s.last_restock
        )
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id
    FROM eligible
    EXCEPT
    SELECT id
    FROM stores
    WHERE region IN ('Central', 'East')
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM eligible s
    INNER JOIN remaining_keys k ON s.id = k.id
)
SELECT
    id,
    store_name,
    region,
    opened_date,
    sqft_band,
    city_group,
    prev_sqft,
    quantity,
    last_restock
FROM reconciled
ORDER BY prev_sqft NULLS FIRST;
