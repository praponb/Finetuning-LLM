WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        last_restock,
        ADD_MONTHS(last_restock, 12) AS last_restock_plus_months
    FROM inventory
    WHERE last_restock > DATE '2023-08-16'
),
enriched AS (
    -- Attach stores attributes
    SELECT
        s.*,
        d.store_name,
        d.opened_date,
        d.region
    FROM base s
    JOIN stores d
        ON s.store_id = d.id
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_id,
        last_restock,
        last_restock_plus_months,
        store_name,
        opened_date,
        region,
        LAST_VALUE(region, TRUE) OVER (PARTITION BY region ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_region,
        ROW_NUMBER() OVER w_running AS seq_no,
        ROW_NUMBER() OVER (ORDER BY opened_date DESC, id) AS row_seq,
        LAST_VALUE(region) IGNORE NULLS OVER (ORDER BY last_restock_plus_months, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_region_2
    FROM enriched
    WINDOW w_running AS (ORDER BY last_restock, id)
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM with_windows
    QUALIFY ROW_NUMBER() OVER (PARTITION BY store_id, region ORDER BY last_restock_plus_months, id) = 1
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        ROUND(row_seq / SUM(row_seq) OVER (PARTITION BY region), 4) AS row_seq_share,
        MAX(seq_no) OVER (PARTITION BY region) AS region_max_seq_no,
        NTILE(4) OVER (PARTITION BY store_id ORDER BY seq_no, id) AS seq_no_bucket,
        NTILE(3) OVER w_running AS row_seq_bucket
    FROM latest
    WINDOW w_running AS (PARTITION BY store_id ORDER BY row_seq DESC, id)
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        store_id,
        last_restock,
        last_restock_plus_months,
        store_name,
        opened_date,
        region,
        last_known_region,
        seq_no,
        row_seq,
        last_known_region_2,
        row_seq_share,
        region_max_seq_no,
        seq_no_bucket,
        row_seq_bucket,
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY last_restock_plus_months, id) AS event_seq,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY row_seq_bucket, id) AS event_seq_2,
        COUNT(seq_no) OVER (PARTITION BY store_id) AS store_id_count_seq_no
    FROM metrics
),
by_type AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            store_id,
            region,
            store_id_count_seq_no
        FROM scored
    ) src
    PIVOT (
        SUM(store_id_count_seq_no) AS total
        FOR region IN ('Central' AS central, 'East' AS east)
    )
)
SELECT store_id, east
FROM by_type
ORDER BY store_id DESC NULLS LAST;
