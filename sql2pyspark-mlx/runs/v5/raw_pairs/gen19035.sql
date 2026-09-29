WITH with_dims AS (
    SELECT
        a.id,
        a.store_id,
        a.product_id,
        a.quantity,
        d.city
    FROM inventory a
    LEFT JOIN stores d
        ON a.store_id = d.id
),
deduped AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id, store_id ORDER BY quantity DESC, id) AS latest_rank
        FROM with_dims
    ) t
    WHERE latest_rank = 1
),
metrics AS (
    SELECT
        id,
        store_id,
        product_id,
        LAG(quantity, 1) OVER w_part AS prev_quantity,
        AVG(quantity) OVER (PARTITION BY store_id) AS store_id_avg_quantity,
        AVG(quantity) OVER (PARTITION BY store_id ORDER BY latest_rank, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS quantity_avg_6_prior
    FROM deduped
    WINDOW w_part AS (PARTITION BY store_id ORDER BY latest_rank, id)
),
with_windows AS (
    SELECT
        id,
        store_id,
        prev_quantity,
        quantity_avg_6_prior,
        PERCENT_RANK() OVER (PARTITION BY product_id ORDER BY prev_quantity DESC NULLS LAST) AS prev_quantity_percent_rank,
        DENSE_RANK() OVER (PARTITION BY product_id ORDER BY quantity_avg_6_prior) AS quantity_avg_6_prior_dense_rank
    FROM metrics
),
joined AS (
    SELECT
        src.*,
        s.region,
        s.opened_date
    FROM with_windows src
    LEFT JOIN stores s
        ON src.store_id = s.id
),
matched_keys AS (
    SELECT store_id, id
    FROM joined
    EXCEPT
    SELECT store_id, id
    FROM inventory
    WHERE quantity BETWEEN 163 AND 233
),
reconciled AS (
    SELECT s.*
    FROM joined s
    JOIN matched_keys k ON s.store_id = k.store_id AND s.id = k.id
)
SELECT
    id,
    store_id,
    quantity_avg_6_prior,
    quantity_avg_6_prior_dense_rank,
    region,
    opened_date
FROM reconciled
