WITH scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        status,
        discount,
        shipping_fee,
        CORR(shipping_fee, discount) OVER w_part corr_shipping_fee_discount,
        AVG(shipping_fee) OVER (PARTITION BY customer_id, status ORDER BY shipping_fee, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) shipping_fee_avg_6,
        CUME_DIST() OVER (PARTITION BY customer_id ORDER BY discount DESC) discount_cume_dist
    FROM orders
    WHERE status != 'SHIPPED'
    WINDOW w_part AS (PARTITION BY customer_id)
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        customer_id,
        discount,
        discount_cume_dist
    FROM scored
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY shipping_fee DESC NULLS FIRST, id) = 1
),
ranked_orders AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY discount_cume_dist DESC NULLS LAST, id) AS dedup_rank
        FROM ranked
    ) t
    WHERE dedup_rank = 1
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        FIRST_VALUE(discount) IGNORE NULLS OVER (PARTITION BY customer_id ORDER BY discount_cume_dist, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_discount,
        LAST_VALUE(discount) IGNORE NULLS OVER (ORDER BY discount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_discount,
        MAX(discount) OVER (PARTITION BY customer_id) customer_id_max_discount,
        LEAD(discount, 1) OVER (PARTITION BY customer_id ORDER BY dedup_rank, id) next_discount
    FROM ranked_orders
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        discount_cume_dist,
        first_known_discount,
        customer_id_max_discount,
        next_discount
    FROM with_windows
    QUALIFY RANK() OVER (PARTITION BY customer_id ORDER BY first_known_discount DESC NULLS FIRST) = 1
),
filtered AS (
    -- Keep recent, valid rows
    SELECT *, customer_id_max_discount * next_discount customer_id_max_discount_times_next_discount
    FROM deduped
    WHERE next_discount < 0.21
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        discount_cume_dist,
        customer_id_max_discount,
        next_discount,
        customer_id_max_discount_times_next_discount,
        NTILE(4) OVER w_running customer_id_max_discount_times_next_discount_bucket,
        SUM(customer_id_max_discount) OVER w_recent cumulative_customer_id_max_discount,
        LEAD(customer_id_max_discount_times_next_discount, 2) OVER w_grp next_customer_id_max_discount_times_next_discount
    FROM filtered
    WINDOW w_running AS (ORDER BY customer_id_max_discount_times_next_discount DESC, id), w_recent AS (ORDER BY first_known_discount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_grp AS (ORDER BY first_known_discount, id)
)
SELECT
    id,
    discount_cume_dist,
    customer_id_max_discount,
    customer_id_max_discount_times_next_discount,
    customer_id_max_discount_times_next_discount_bucket,
    next_customer_id_max_discount_times_next_discount
FROM enriched
