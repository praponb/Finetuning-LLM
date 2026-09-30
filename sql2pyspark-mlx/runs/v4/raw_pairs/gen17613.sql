WITH metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        NTILE(5) OVER w_ordered AS cost_bucket,
        NTILE(10) OVER w_ordered AS cost_bucket_2,
        NTILE(3) OVER w_recent AS cost_bucket_3
    FROM shipments
    WINDOW w_ordered AS (PARTITION BY order_id ORDER BY cost DESC, id), w_recent AS (PARTITION BY carrier, order_id ORDER BY cost, id)
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY cost DESC NULLS FIRST) AS cost_cume_dist,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY cost_bucket_2, id) AS event_seq,
        LAST_VALUE(cost_bucket) IGNORE NULLS OVER w_recent AS last_known_cost_bucket
    FROM metrics
    WINDOW w_recent AS (PARTITION BY order_id ORDER BY cost_bucket_3, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        a.*,
        b.order_date,
        b.shipping_fee
    FROM with_windows a
    JOIN orders b
        ON a.order_id = b.id
),
metrics_shipments AS (
    -- Running and moving aggregates
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        cost_bucket,
        cost_bucket_2,
        cost_bucket_3,
        cost_cume_dist,
        event_seq,
        last_known_cost_bucket,
        order_date,
        shipping_fee,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY cost_cume_dist, id) AS row_seq,
        CUME_DIST() OVER (PARTITION BY carrier ORDER BY event_seq DESC) AS event_seq_cume_dist
    FROM with_dims
),
joined AS (
    -- Attach orders attributes
    SELECT
        s.*,
        b.amount,
        b.discount
    FROM metrics_shipments s
    JOIN orders b
        ON s.order_id = b.id
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(event_seq) OVER (PARTITION BY carrier, status ORDER BY discount, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS event_seq_avg_3_prior,
        LEAD(event_seq_cume_dist, 1, 0) OVER w_recent AS next_event_seq_cume_dist
    FROM joined
    WINDOW w_recent AS (PARTITION BY carrier ORDER BY order_date, id)
),
enriched_shipments AS (
    -- Ranking and gap features
    SELECT
        *,
        ROUND(event_seq_avg_3_prior / SUM(event_seq_avg_3_prior) OVER (PARTITION BY carrier), 4) AS event_seq_avg_3_prior_share,
        AVG(discount) OVER (ORDER BY event_seq, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS discount_avg_6,
        AVG(event_seq_cume_dist) OVER (PARTITION BY status ORDER BY event_seq_cume_dist, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS event_seq_cume_dist_avg_2_prior,
        FIRST_VALUE(next_event_seq_cume_dist) IGNORE NULLS OVER (PARTITION BY status ORDER BY event_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_next_event_seq_cume_dist,
        ROUND(event_seq / SUM(event_seq) OVER (PARTITION BY carrier), 4) AS event_seq_share
    FROM enriched
)
SELECT
    id,
    carrier,
    shipped_date,
    delivered_date,
    cost,
    status,
    cost_bucket,
    cost_bucket_3,
    event_seq
FROM enriched_shipments
