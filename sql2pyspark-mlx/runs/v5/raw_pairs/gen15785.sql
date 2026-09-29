WITH with_dims AS (
    SELECT
        a.*,
        b.opened_date,
        b.region,
        b.store_name
    FROM inventory a
    INNER JOIN stores b
        ON a.store_id = b.id
),
deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY opened_date DESC NULLS FIRST, id) AS row_num
    FROM with_dims
    QUALIFY row_num = 1
),
joined AS (
    SELECT
        src.id,
        src.store_id,
        src.product_id,
        src.opened_date,
        src.region,
        d.region AS store_region,
        d.city,
        d.sqft
    FROM deduped src
    INNER JOIN stores d
        ON src.store_id = d.id
    WHERE d.opened_date >= '2023-09-04'
),
scored AS (
    SELECT
        id,
        region,
        store_region,
        city,
        sqft,
        LAG(sqft) OVER (PARTITION BY region ORDER BY opened_date, id) AS prev_sqft,
        LEAD(sqft) OVER (PARTITION BY region ORDER BY opened_date, id) AS next_sqft,
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY opened_date, id) AS event_seq,
        ROW_NUMBER() OVER (PARTITION BY store_id, city ORDER BY opened_date, id) AS seq_no
    FROM joined
),
enriched AS (
    SELECT
        id,
        region,
        city,
        sqft,
        ROUND(seq_no / SUM(seq_no) OVER (PARTITION BY city), 4) AS seq_no_share,
        FIRST_VALUE(region) IGNORE NULLS OVER (ORDER BY event_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_region,
        LAG(prev_sqft, 2) OVER (PARTITION BY store_region, region ORDER BY next_sqft, id) AS prev_prev_sqft
    FROM scored
),
with_windows AS (
    SELECT
        id,
        region,
        city,
        sqft,
        first_known_region,
        SUM(sqft) OVER (PARTITION BY city, region ORDER BY seq_no_share, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_sum_sqft,
        MAX(sqft) OVER (PARTITION BY city, region ORDER BY seq_no_share, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_max_sqft,
        AVG(sqft) OVER (PARTITION BY city, region ORDER BY seq_no_share, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_avg_sqft,
        MIN(sqft) OVER (PARTITION BY city, region ORDER BY seq_no_share, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_min_sqft,
        LEAD(prev_prev_sqft, 2, 0) OVER (PARTITION BY region ORDER BY sqft, id) AS next_prev_prev_sqft
    FROM enriched
),
matched_keys AS (
    SELECT id
    FROM with_windows
    EXCEPT
    SELECT id
    FROM inventory
    WHERE last_restock BETWEEN '2023-08-01' AND '2025-10-01'
),
reconciled AS (
    SELECT s.*
    FROM with_windows s
    INNER JOIN matched_keys k ON s.id = k.id
)
SELECT *
FROM (
    SELECT
        region,
        city,
        last3_max_sqft
    FROM reconciled
) src
PIVOT (
    SUM(last3_max_sqft)
    FOR city IN ('Bangkok', 'Berlin', 'Tokyo')
)
