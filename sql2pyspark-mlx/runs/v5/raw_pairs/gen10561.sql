WITH joined AS (
    SELECT
        e.*,
        d.shipping_fee,
        d.order_date
    FROM order_items e
    LEFT JOIN orders d
        ON e.order_id = d.id
    WHERE d.discount BETWEEN 0.14 AND 0.38
),
metrics AS (
    SELECT
        *,
        AVG(unit_price) OVER (PARTITION BY product_id ORDER BY unit_price, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS unit_price_avg_2,
        AVG(unit_price) OVER (PARTITION BY order_id, product_id) AS order_id_avg_unit_price,
        COUNT(shipping_fee) OVER (PARTITION BY product_id, order_id) AS product_id_count_shipping_fee,
        PERCENT_RANK() OVER (PARTITION BY order_id, product_id ORDER BY quantity DESC NULLS FIRST) AS quantity_percent_rank,
        RANK() OVER (PARTITION BY product_id ORDER BY unit_price DESC NULLS FIRST) AS unit_price_rank
    FROM joined
),
metrics_order_items AS (
    SELECT
        id,
        order_id,
        product_id,
        unit_price,
        shipping_fee,
        order_date,
        unit_price_avg_2,
        order_id_avg_unit_price,
        unit_price_rank,
        MAX(unit_price_rank) OVER (PARTITION BY product_id, order_id ORDER BY unit_price_avg_2, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_max_unit_price_rank,
        MIN(unit_price_rank) OVER (PARTITION BY product_id, order_id ORDER BY unit_price_avg_2, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_min_unit_price_rank,
        SUM(unit_price_rank) OVER (PARTITION BY product_id, order_id ORDER BY unit_price_avg_2, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_sum_unit_price_rank,
        AVG(quantity_percent_rank) OVER (PARTITION BY order_id ORDER BY quantity, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS quantity_percent_rank_avg_2
    FROM metrics
),
joined_order_items AS (
    SELECT
        a.id,
        a.order_id,
        a.product_id,
        a.unit_price,
        a.order_date,
        a.unit_price_avg_2,
        a.order_id_avg_unit_price,
        a.last7_max_unit_price_rank,
        a.quantity_percent_rank_avg_2,
        b.status,
        b.discount,
        b.order_date AS order_order_date
    FROM metrics_order_items a
    JOIN orders b
        ON a.order_id = b.id
),
ranked AS (
    SELECT *
    FROM joined_order_items
    QUALIFY RANK() OVER (PARTITION BY product_id ORDER BY unit_price_avg_2 DESC) = 1
),
enriched AS (
    SELECT
        id,
        order_id,
        product_id,
        order_date,
        last7_max_unit_price_rank,
        ROUND(last7_max_unit_price_rank / SUM(last7_max_unit_price_rank) OVER (PARTITION BY order_id), 4) AS last7_max_unit_price_rank_share,
        AVG(last7_max_unit_price_rank) OVER (PARTITION BY order_id, status ORDER BY order_order_date, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS last7_max_unit_price_rank_avg_5_prior,
        NTILE(10) OVER (PARTITION BY product_id ORDER BY quantity_percent_rank_avg_2 DESC, id) AS quantity_percent_rank_avg_2_bucket,
        SUM(quantity_percent_rank_avg_2) OVER (PARTITION BY product_id ORDER BY order_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_quantity_percent_rank_avg_2
    FROM ranked
),
with_windows AS (
    SELECT
        *,
        MIN(last7_max_unit_price_rank_avg_5_prior) OVER w_ordered AS last3_min_last7_max_unit_price_rank_avg_5_prior,
        COUNT(last7_max_unit_price_rank_avg_5_prior) OVER w_ordered AS last3_count_last7_max_unit_price_rank_avg_5_prior,
        MAX(last7_max_unit_price_rank_avg_5_prior) OVER w_ordered AS last3_max_last7_max_unit_price_rank_avg_5_prior
    FROM enriched
    WINDOW w_ordered AS (PARTITION BY product_id, order_id ORDER BY quantity_percent_rank_avg_2_bucket, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    last7_max_unit_price_rank_share,
    last7_max_unit_price_rank_avg_5_prior,
    quantity_percent_rank_avg_2_bucket,
    cumulative_quantity_percent_rank_avg_2,
    last3_min_last7_max_unit_price_rank_avg_5_prior,
    last3_count_last7_max_unit_price_rank_avg_5_prior,
    last3_max_last7_max_unit_price_rank_avg_5_prior
FROM with_windows
ORDER BY last3_max_last7_max_unit_price_rank_avg_5_prior NULLS LAST;
