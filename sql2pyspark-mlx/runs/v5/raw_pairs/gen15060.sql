WITH cleaned AS (
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        DATE_FORMAT(last_restock, 'yyyy-MM-dd') AS last_restock_label,
        DATEDIFF(CURRENT_DATE(), last_restock) AS days_since_last_restock
    FROM inventory
    WHERE last_restock > TO_DATE('2023-04-01')
      AND quantity < 181
),
enriched AS (
    SELECT
        *,
        CUME_DIST() OVER w_part AS quantity_cume_dist,
        NTILE(5) OVER w_hist AS quantity_bucket,
        LAG(days_since_last_restock, 2) OVER w_hist AS prev_days_since_last_restock,
        ROW_NUMBER() OVER w_ordered AS event_seq
    FROM cleaned
    WINDOW w_part AS (PARTITION BY store_id ORDER BY quantity NULLS LAST), w_hist AS (ORDER BY quantity, id), w_ordered AS (PARTITION BY product_id ORDER BY last_restock, id)
),
joined AS (
    SELECT
        src.id,
        src.store_id,
        src.quantity,
        src.last_restock,
        src.last_restock_label,
        src.days_since_last_restock,
        src.quantity_cume_dist,
        src.prev_days_since_last_restock,
        src.event_seq,
        b.opened_date,
        b.store_name
    FROM enriched src
    LEFT JOIN stores b
        ON src.store_id = b.id
    WHERE b.opened_date BETWEEN '2023-11-01' AND '2024-11-01'
),
with_dims AS (
    SELECT
        src.id,
        src.store_id,
        src.last_restock,
        src.last_restock_label,
        src.days_since_last_restock,
        src.prev_days_since_last_restock,
        src.opened_date,
        d.city
    FROM joined src
    JOIN stores d
        ON src.store_id = d.id
),
scored AS (
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY days_since_last_restock, id) AS seq_no,
        AVG(prev_days_since_last_restock) OVER (ORDER BY prev_days_since_last_restock, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS prev_days_since_last_restock_avg_6
    FROM with_dims
)
SELECT *
FROM (
    SELECT
        store_id,
        city,
        prev_days_since_last_restock_avg_6
    FROM scored
) src
PIVOT (
    MAX(prev_days_since_last_restock_avg_6) AS total,
    COUNT(1) AS item_count
    FOR city IN ('Berlin', 'Tokyo')
);
