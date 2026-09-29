WITH valid AS (
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        COALESCE(quantity, 0) AS quantity_filled
    FROM order_items
    WHERE product_id IS NOT NULL
),
scored AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY quantity DESC, id) AS row_seq,
        PERCENT_RANK() OVER (PARTITION BY order_id ORDER BY quantity_filled DESC NULLS FIRST) AS quantity_filled_percent_rank
    FROM valid
),
with_windows AS (
    SELECT
        *,
        FIRST_VALUE(quantity_filled_percent_rank, TRUE) OVER (ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_quantity_filled_percent_rank,
        AVG(row_seq) OVER (PARTITION BY product_id, order_id ORDER BY row_seq, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS row_seq_avg_6_prior,
        NTILE(4) OVER (PARTITION BY order_id, product_id ORDER BY row_seq, id) AS row_seq_bucket,
        ROW_NUMBER() OVER (ORDER BY row_seq DESC, id) AS event_seq
    FROM scored
),
cleaned AS (
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        quantity_filled,
        first_known_quantity_filled_percent_rank,
        row_seq_avg_6_prior,
        FLOOR(row_seq_bucket) AS row_seq_bucket_whole
    FROM with_windows
    WHERE product_id IS NOT NULL
      AND unit_price > 390
),
enriched AS (
    SELECT
        *,
        LAG(row_seq_bucket_whole, 1) OVER (ORDER BY quantity_filled, id) AS prev_row_seq_bucket_whole,
        CUME_DIST() OVER (PARTITION BY product_id, order_id ORDER BY quantity_filled DESC) AS quantity_filled_cume_dist,
        FIRST_VALUE(unit_price, TRUE) OVER (ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_unit_price,
        LEAD(row_seq_bucket_whole, 1) OVER (ORDER BY first_known_quantity_filled_percent_rank, id) AS next_row_seq_bucket_whole,
        AVG(row_seq_bucket_whole) OVER (PARTITION BY order_id) AS order_id_avg_row_seq_bucket_whole
    FROM cleaned
),
enriched_order_items AS (
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        quantity_filled,
        first_known_quantity_filled_percent_rank,
        row_seq_avg_6_prior,
        row_seq_bucket_whole,
        prev_row_seq_bucket_whole,
        quantity_filled_cume_dist,
        first_known_unit_price,
        next_row_seq_bucket_whole,
        order_id_avg_row_seq_bucket_whole,
        LAST_VALUE(prev_row_seq_bucket_whole) IGNORE NULLS OVER (PARTITION BY product_id, order_id ORDER BY row_seq_bucket_whole, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_prev_row_seq_bucket_whole,
        SUM(quantity_filled_cume_dist) OVER (PARTITION BY order_id ORDER BY first_known_unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_quantity_filled_cume_dist,
        SUM(first_known_unit_price) OVER (PARTITION BY product_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_first_known_unit_price,
        AVG(order_id_avg_row_seq_bucket_whole) OVER (PARTITION BY order_id ORDER BY prev_row_seq_bucket_whole, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS order_id_avg_row_seq_bucket_whole_avg_5_prior
    FROM enriched
),
joined AS (
    SELECT
        src.*,
        p.category,
        p.price
    FROM enriched_order_items src
    LEFT JOIN products p
        ON src.product_id = p.id
),
daily AS (
    SELECT
        CASE WHEN quantity_filled_cume_dist >= 0.62 THEN 'LARGE' ELSE 'SMALL' END AS quantity_filled_cume_dist_tier,
        category,
        BIT_OR(quantity) AS quantity_bit_or,
        PERCENTILE_APPROX(running_first_known_unit_price, 0.5) AS running_first_known_unit_price_p50,
        SORT_ARRAY(COLLECT_SET(category)) AS category_list,
        SORT_ARRAY(COLLECT_SET(category)) AS category_list_2,
        SIZE(COLLECT_SET(category)) AS num_categories
    FROM joined
    GROUP BY CASE WHEN quantity_filled_cume_dist >= 0.62 THEN 'LARGE' ELSE 'SMALL' END, category
)
SELECT
    quantity_filled_cume_dist_tier,
    category,
    running_first_known_unit_price_p50,
    category_list,
    category_list_2,
    num_categories
FROM daily
ORDER BY num_categories;
