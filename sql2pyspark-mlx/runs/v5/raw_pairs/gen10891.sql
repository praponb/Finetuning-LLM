WITH with_dims AS (
    SELECT
        src.id,
        src.store_id,
        src.product_id,
        src.quantity,
        s.city,
        s.store_name
    FROM inventory src
    LEFT JOIN stores s
        ON src.store_id = s.id
),
scored AS (
    SELECT
        *,
        SUM(quantity) OVER w_running AS cumulative_quantity,
        AVG(quantity) OVER (PARTITION BY product_id, city ORDER BY quantity, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS quantity_avg_6_prior,
        AVG(quantity) OVER (ORDER BY quantity, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS quantity_avg_5_prior
    FROM with_dims
    WINDOW w_running AS (PARTITION BY product_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
deduped AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY quantity NULLS LAST, id) AS rn
        FROM scored
    ) ranked
    WHERE rn = 1
),
with_windows AS (
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        city,
        store_name,
        quantity_avg_6_prior,
        quantity_avg_5_prior,
        rn,
        PERCENT_RANK() OVER w_recent AS quantity_avg_5_prior_percent_rank,
        AVG(quantity_avg_5_prior) OVER w_part AS quantity_avg_5_prior_avg_3_prior,
        NTILE(4) OVER (PARTITION BY product_id ORDER BY quantity DESC, id) AS quantity_bucket,
        AVG(quantity_avg_5_prior) OVER (PARTITION BY product_id, city ORDER BY rn, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS quantity_avg_5_prior_avg_6_prior,
        LAST_VALUE(quantity_avg_5_prior) IGNORE NULLS OVER (PARTITION BY store_id ORDER BY quantity_avg_6_prior, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_quantity_avg_5_prior
    FROM deduped
    WINDOW w_recent AS (PARTITION BY product_id ORDER BY quantity_avg_5_prior NULLS LAST), w_part AS (PARTITION BY city, product_id ORDER BY cumulative_quantity, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING)
),
scored_inventory AS (
    SELECT
        id,
        product_id,
        city,
        store_name,
        quantity_avg_5_prior_percent_rank,
        quantity_avg_5_prior_avg_3_prior,
        quantity_bucket,
        quantity_avg_5_prior_avg_6_prior,
        last_known_quantity_avg_5_prior,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity_avg_5_prior_avg_6_prior, id) AS event_seq,
        LAST_VALUE(last_known_quantity_avg_5_prior, TRUE) OVER (PARTITION BY store_id ORDER BY quantity_avg_5_prior, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_last_known_quantity_avg_5_prior,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY quantity_avg_5_prior, id) AS row_seq,
        DENSE_RANK() OVER (PARTITION BY store_id, city ORDER BY quantity_avg_5_prior_percent_rank DESC NULLS LAST) AS quantity_avg_5_prior_percent_rank_dense_rank
    FROM with_windows
    WHERE quantity_bucket <= 7
)
SELECT
    id,
    city,
    store_name,
    quantity_avg_5_prior_percent_rank,
    quantity_avg_5_prior_avg_6_prior,
    last_known_quantity_avg_5_prior,
    event_seq,
    last_known_last_known_quantity_avg_5_prior,
    row_seq,
    CASE WHEN city RLIKE '^(Berlin|Tokyo)$' THEN 'GROUP_A' ELSE 'GROUP_B' END AS city_zone
FROM scored_inventory
ORDER BY last_known_quantity_avg_5_prior DESC NULLS FIRST
