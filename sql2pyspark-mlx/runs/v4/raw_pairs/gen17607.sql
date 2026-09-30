WITH base AS (
    SELECT
        id,
        store_id,
        DATEDIFF(CURRENT_DATE(), last_restock) AS days_since_last_restock,
        DATE_TRUNC('month', last_restock) AS last_restock_period
    FROM inventory
    WHERE last_restock >= DATE '2023-07-16'
),
scored AS (
    SELECT
        id,
        store_id,
        days_since_last_restock,
        last_restock_period,
        AVG(days_since_last_restock) OVER (PARTITION BY store_id) AS store_id_avg_days_since_last_restock,
        MAX(days_since_last_restock) OVER (PARTITION BY store_id ORDER BY CAST(last_restock_period AS BIGINT) ASC RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS days_since_last_restock_max_7d
    FROM base
),
enriched AS (
    SELECT a.*, b.store_name
    FROM scored a
    INNER JOIN stores b
        ON a.store_id = b.id
),
enriched_inventory AS (
    SELECT
        *,
        AVG(days_since_last_restock) OVER (PARTITION BY store_id ORDER BY store_id_avg_days_since_last_restock ASC, id ASC ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS days_since_last_restock_avg_3_prior,
        ROW_NUMBER() OVER (ORDER BY last_restock_period ASC, id ASC) AS event_seq
    FROM enriched
),
scored_inventory AS (
    SELECT
        *,
        FIRST_VALUE(days_since_last_restock_avg_3_prior, TRUE) OVER w_part AS first_known_days_since_last_restock_avg_3_prior,
        FIRST_VALUE(store_id_avg_days_since_last_restock) IGNORE NULLS OVER (PARTITION BY store_id ORDER BY days_since_last_restock_avg_3_prior ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_store_id_avg_days_since_last_restock
    FROM enriched_inventory
    WINDOW w_part AS (PARTITION BY store_id ORDER BY days_since_last_restock_max_7d ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    s.id,
    s.store_id,
    s.days_since_last_restock,
    s.last_restock_period,
    s.store_id_avg_days_since_last_restock,
    s.days_since_last_restock_max_7d,
    s.store_name,
    s.first_known_days_since_last_restock_avg_3_prior,
    CASE WHEN store_name RLIKE '^(Downtown Express|Old Town)$' THEN 'EXTENDED' WHEN store_name RLIKE '^(Siam Flagship|Airport Kiosk)$' THEN 'GROUP_A' ELSE 'OTHER_ZONE' END AS store_name_zone,
    (SELECT MIN(o2.shipping_fee) FROM orders o2 WHERE o2.store_id = s.store_id) AS min_order_shipping_fee
FROM scored_inventory s
ORDER BY id ASC NULLS FIRST, min_order_shipping_fee DESC;
