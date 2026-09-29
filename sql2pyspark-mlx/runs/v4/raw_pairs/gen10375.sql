WITH enriched AS (
    SELECT
        s.*,
        o.discount,
        o.status
    FROM order_items s
    JOIN orders o
        ON s.order_id = o.id
),
with_windows AS (
    SELECT
        *,
        NTILE(5) OVER (ORDER BY unit_price, id) AS unit_price_bucket,
        NTILE(3) OVER (ORDER BY discount, id) AS discount_bucket
    FROM enriched
),
latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY unit_price DESC NULLS FIRST, id) AS rank_in_group
    FROM with_windows
    QUALIFY rank_in_group = 1
),
with_dims AS (
    SELECT
        a.*,
        p.product_name,
        p.price
    FROM latest a
    LEFT JOIN products p
        ON a.product_id = p.id
),
cleaned AS (
    SELECT
        id,
        product_id,
        quantity,
        unit_price,
        discount,
        status,
        unit_price_bucket,
        rank_in_group,
        product_name,
        price * discount AS price_times_discount
    FROM with_dims
    WHERE LOWER(TRIM(product_name)) = 'yoga mat'
),
scored AS (
    SELECT
        *,
        NTILE(3) OVER (PARTITION BY status, product_id ORDER BY quantity DESC, id) AS quantity_bucket,
        AVG(price_times_discount) OVER (PARTITION BY product_id, status) AS product_id_avg_price_times_discount,
        ROUND(unit_price / SUM(unit_price) OVER (PARTITION BY product_id), 4) AS unit_price_share,
        NTILE(3) OVER (PARTITION BY status, product_id ORDER BY price_times_discount, id) AS price_times_discount_bucket,
        NTILE(10) OVER (PARTITION BY product_id ORDER BY price_times_discount, id) AS price_times_discount_bucket_2
    FROM cleaned
),
latest_order_items AS (
    SELECT *, RANK() OVER (PARTITION BY status ORDER BY price_times_discount_bucket DESC) AS rank_in_group_2
    FROM scored
    QUALIFY rank_in_group_2 = 1
)
SELECT
    *,
    NTILE(4) OVER (PARTITION BY product_id ORDER BY unit_price_bucket DESC, id) AS unit_price_bucket_bucket,
    ROW_NUMBER() OVER w_hist AS seq_no,
    NTILE(10) OVER (PARTITION BY status ORDER BY quantity DESC, id) AS quantity_bucket_2
FROM latest_order_items
WINDOW w_hist AS (ORDER BY rank_in_group_2 DESC, id);
