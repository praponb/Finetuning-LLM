WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        DATE_TRUNC('week', last_restock) AS last_restock_period,
        LAST_DAY(last_restock) AS last_restock_month_end
    FROM inventory
    WHERE last_restock > DATE '2024-01-31'
      AND product_id IN (2, 5, 10)
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        last_restock_period,
        last_restock_month_end,
        ROW_NUMBER() OVER w_part AS seq_no,
        ROW_NUMBER() OVER (PARTITION BY product_id, store_id ORDER BY last_restock DESC, id) AS event_seq,
        COUNT(quantity) OVER w_part_2 AS quantity_count_7d,
        ROW_NUMBER() OVER (ORDER BY last_restock, id) AS seq_no_2
    FROM valid
    WINDOW w_part AS (PARTITION BY store_id ORDER BY quantity, id), w_part_2 AS (PARTITION BY store_id ORDER BY CAST(last_restock_period AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW)
),
joined AS (
    -- Attach stores attributes
    SELECT
        a.*,
        s.city,
        s.store_name,
        s.sqft
    FROM scored a
    LEFT JOIN stores s
        ON a.store_id = s.id
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT product_id, id
    FROM joined
    INTERSECT ALL
    SELECT product_id, id
    FROM inventory
    WHERE store_id IN (1, 4, 5, 6)
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM joined s
    INNER JOIN remaining_keys k ON s.product_id = k.product_id AND s.id = k.id
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        last_restock_period,
        last_restock_month_end,
        seq_no,
        event_seq,
        quantity_count_7d,
        seq_no_2,
        city,
        store_name,
        sqft,
        ROUND(seq_no_2 / SUM(seq_no_2) OVER (PARTITION BY product_id), 4) AS seq_no_2_share,
        MAX(seq_no) OVER (PARTITION BY store_id) AS store_id_max_seq_no,
        SUM(quantity) OVER (PARTITION BY city ORDER BY CAST(last_restock_period AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS quantity_sum_30d,
        MIN(sqft) OVER (PARTITION BY city) AS city_min_sqft
    FROM kept_rows
)
-- Window metrics over several different partitions
SELECT
    *,
    AVG(event_seq) OVER (PARTITION BY city) AS city_avg_event_seq,
    CUME_DIST() OVER (ORDER BY quantity_count_7d DESC NULLS LAST) AS quantity_count_7d_cume_dist,
    AVG(seq_no_2) OVER (ORDER BY CAST(last_restock_period AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS seq_no_2_avg_90d,
    RANK() OVER w_ordered AS quantity_count_7d_rank
FROM metrics
WINDOW w_ordered AS (PARTITION BY product_id ORDER BY quantity_count_7d NULLS FIRST)
