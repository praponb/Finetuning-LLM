WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_id,
        order_date,
        status,
        shipping_fee,
        LOWER(channel) AS channel_lower
    FROM orders
    WHERE order_date > TO_DATE('2024-02-11')
),
joined AS (
    -- Attach stores attributes
    SELECT
        src.*,
        b.opened_date,
        b.sqft,
        b.city
    FROM filtered src
    LEFT JOIN stores b
        ON src.store_id = b.id
),
joined_orders AS (
    -- Attach stores attributes
    SELECT
        e.*,
        d.sqft AS store_sqft,
        d.region
    FROM joined e
    LEFT JOIN stores d
        ON e.store_id = d.id
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(shipping_fee) OVER (ORDER BY store_sqft, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS shipping_fee_avg_2_prior,
        NTILE(10) OVER (PARTITION BY city ORDER BY sqft DESC, id) AS sqft_bucket,
        LEAD(store_sqft, 2) OVER (PARTITION BY city ORDER BY opened_date, id) AS next_store_sqft,
        LEAD(shipping_fee, 1) OVER (PARTITION BY store_id ORDER BY shipping_fee, id) AS next_shipping_fee,
        SUM(store_sqft) OVER (PARTITION BY region ORDER BY order_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_store_sqft
    FROM joined_orders
),
enriched_orders AS (
    -- Attach stores attributes
    SELECT e.*, b.store_name
    FROM enriched e
    LEFT JOIN stores b
        ON e.store_id = b.id
)
SELECT
    id,
    order_date,
    status,
    shipping_fee,
    channel_lower,
    opened_date,
    sqft,
    store_sqft,
    shipping_fee_avg_2_prior
FROM enriched_orders
ORDER BY shipping_fee NULLS LAST;
