WITH latest AS (
    SELECT *
    FROM inventory
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY last_restock DESC NULLS LAST, id) = 1
),
with_windows AS (
    SELECT
        *,
        AVG(quantity) OVER w_recent AS quantity_avg_3_prior,
        COUNT(quantity) OVER (PARTITION BY product_id, store_id) AS product_id_count_quantity,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity, id) AS seq_no
    FROM latest
    WHERE quantity > 189
    WINDOW w_recent AS (PARTITION BY product_id ORDER BY quantity, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING)
),
with_dims AS (
    SELECT
        a.id,
        a.store_id,
        a.product_id_count_quantity,
        a.seq_no,
        s.region
    FROM with_windows a
    JOIN stores s
        ON a.store_id = s.id
),
scored AS (
    SELECT
        id,
        store_id,
        product_id_count_quantity,
        LEAD(seq_no) OVER w_hist AS next_seq_no,
        LEAD(product_id_count_quantity) OVER w_hist AS next_product_id_count_quantity,
        ROW_NUMBER() OVER w_hist AS row_seq,
        MIN(seq_no) OVER (PARTITION BY store_id) AS store_id_min_seq_no
    FROM with_dims
    WINDOW w_hist AS (PARTITION BY region ORDER BY seq_no, id)
),
base AS (
    SELECT *, COALESCE(row_seq, 0) AS row_seq_filled
    FROM scored
    WHERE store_id_min_seq_no > 6
)
SELECT
    id,
    next_product_id_count_quantity,
    store_id_min_seq_no,
    row_seq_filled,
    RANK() OVER (PARTITION BY store_id ORDER BY row_seq NULLS LAST) AS row_seq_rank,
    FIRST_VALUE(product_id_count_quantity) IGNORE NULLS OVER (PARTITION BY store_id ORDER BY store_id_min_seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_product_id_count_quantity,
    LAG(next_product_id_count_quantity, 1) OVER (PARTITION BY store_id ORDER BY row_seq, id) AS prev_next_product_id_count_quantity,
    MIN(row_seq_filled) OVER (PARTITION BY store_id) AS store_id_min_row_seq_filled,
    LEAD(next_seq_no, 2) OVER (ORDER BY row_seq_filled, id) AS next_next_seq_no
FROM base;
