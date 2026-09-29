WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        LOWER(region) AS region_lower
    FROM stores
    WHERE region IS NOT NULL
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        region_lower,
        ROUND(sqft / SUM(sqft) OVER (PARTITION BY city, region), 4) AS sqft_share,
        AVG(sqft) OVER (ORDER BY sqft, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS sqft_avg_5,
        DENSE_RANK() OVER (PARTITION BY region, city ORDER BY sqft NULLS FIRST) AS sqft_dense_rank
    FROM base
),
with_last AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.last_restock,
        x.quantity
    FROM enriched s
    LEFT JOIN LATERAL (
            SELECT c.last_restock, c.quantity
            FROM inventory c
            WHERE c.store_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        sqft_avg_5,
        last_restock,
        LAG(sqft_avg_5) OVER (ORDER BY last_restock, id) AS prev_sqft_avg_5,
        LEAD(sqft_share) OVER (ORDER BY last_restock, id) AS next_sqft_share,
        LAG(sqft_share) OVER (ORDER BY last_restock, id) AS prev_sqft_share,
        ROW_NUMBER() OVER (ORDER BY last_restock, id) AS seq_no
    FROM with_last
    WHERE quantity != 131
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM scored
    INTERSECT
    SELECT id
    FROM stores
    WHERE store_name LIKE 'M%'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM scored s
    INNER JOIN remaining_keys k ON s.id = k.id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        prev_sqft_avg_5,
        seq_no,
        PERCENT_RANK() OVER w_part AS seq_no_percent_rank,
        DENSE_RANK() OVER w_part_2 AS next_sqft_share_dense_rank
    FROM reconciled
    WINDOW w_part AS (ORDER BY seq_no DESC NULLS FIRST), w_part_2 AS (ORDER BY next_sqft_share DESC NULLS LAST)
)
SELECT
    prev_sqft_avg_5,
    seq_no,
    seq_no_percent_rank
FROM metrics;
