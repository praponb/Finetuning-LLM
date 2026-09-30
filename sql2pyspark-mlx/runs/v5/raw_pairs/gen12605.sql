WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        order_date,
        status,
        channel,
        discount,
        shipping_fee,
        discount + amount discount_plus_amount,
        discount * shipping_fee discount_times_shipping_fee,
        COALESCE(channel, 'OTHER') resolved_channel
    FROM orders
    WHERE order_date >= TO_DATE('2023-02-24')
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct
    FROM valid src
    LEFT JOIN promotions pr
        ON COALESCE(src.channel, 'store') = pr.channel
       AND src.order_date >= pr.starts_on
       AND src.order_date <= DATE_ADD(pr.ends_on, 10)
),
latest_match AS (
    -- Latest matching shipment per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.shipped_date,
        x.shipment_status
    FROM with_window s
    LEFT JOIN LATERAL (
            SELECT c.shipped_date, c.status AS shipment_status
            FROM shipments c
            WHERE c.order_id = s.id
              AND c.shipped_date <= s.order_date
            ORDER BY c.shipped_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(discount) OVER (PARTITION BY status ORDER BY discount, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_discount,
        FIRST_VALUE(shipping_fee) IGNORE NULLS OVER (PARTITION BY promo_code ORDER BY promo_id, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_shipping_fee,
        SUM(shipping_fee) OVER (PARTITION BY status) status_sum_shipping_fee,
        SUM(discount) OVER (ORDER BY discount, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_discount,
        MAX(discount_plus_amount) OVER (PARTITION BY customer_id) customer_id_max_discount_plus_amount
    FROM latest_match
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        order_date,
        channel,
        discount,
        shipping_fee,
        discount_plus_amount,
        discount_times_shipping_fee,
        resolved_channel,
        promo_id,
        promo_code,
        discount_pct,
        shipped_date,
        SUM(first_known_shipping_fee) OVER (PARTITION BY channel, promo_code ORDER BY discount, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_sum_first_known_shipping_fee,
        AVG(first_known_shipping_fee) OVER (PARTITION BY channel, promo_code ORDER BY discount, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_avg_first_known_shipping_fee,
        MIN(first_known_shipping_fee) OVER (PARTITION BY channel, promo_code ORDER BY discount, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_min_first_known_shipping_fee,
        DENSE_RANK() OVER (ORDER BY discount_plus_amount DESC NULLS FIRST) discount_plus_amount_dense_rank
    FROM enriched
),
with_windows_orders AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(running_avg_first_known_shipping_fee) OVER (PARTITION BY customer_id ORDER BY running_avg_first_known_shipping_fee, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_avg_running_avg_first_known_shipping_fee,
        MIN(running_avg_first_known_shipping_fee) OVER (PARTITION BY customer_id ORDER BY running_avg_first_known_shipping_fee, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_min_running_avg_first_known_shipping_fee,
        COUNT(running_avg_first_known_shipping_fee) OVER (PARTITION BY customer_id ORDER BY running_avg_first_known_shipping_fee, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_count_running_avg_first_known_shipping_fee,
        ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY discount_times_shipping_fee DESC, id, promo_id) seq_no
    FROM with_windows
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        order_date,
        discount_plus_amount,
        resolved_channel,
        promo_id,
        running_min_first_known_shipping_fee,
        discount_plus_amount_dense_rank,
        running_min_running_avg_first_known_shipping_fee,
        running_count_running_avg_first_known_shipping_fee,
        seq_no,
        ROW_NUMBER() OVER w_part row_seq,
        FIRST_VALUE(running_sum_first_known_shipping_fee, TRUE) OVER (PARTITION BY customer_id ORDER BY running_min_first_known_shipping_fee, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_running_sum_first_known_shipping_fee
    FROM with_windows_orders
    WHERE running_count_running_avg_first_known_shipping_fee BETWEEN 12 AND 14
    WINDOW w_part AS (PARTITION BY channel, promo_code ORDER BY order_date, id, promo_id)
)
-- Join a dimension and aggregate
SELECT
    p.city,
    p.segment,
    COUNT(*) AS rows_total,
    SUM(src.promo_id) AS total_promo_id,
    MIN(src.discount_plus_amount) AS min_discount_plus_amount,
    GROUPING_ID() AS level_id
FROM scored src
INNER JOIN customers p ON src.customer_id = p.id
GROUP BY CUBE (p.city, p.segment)
ORDER BY city DESC NULLS LAST
