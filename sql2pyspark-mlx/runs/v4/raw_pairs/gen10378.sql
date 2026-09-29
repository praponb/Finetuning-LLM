WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        status,
        amount,
        discount,
        shipping_fee,
        DATE_ADD(order_date, 70) AS order_date_plus_70d
    FROM orders
    WHERE channel IS NOT NULL
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        store_id,
        status,
        amount,
        discount,
        shipping_fee,
        order_date_plus_70d,
        STDDEV_SAMP(shipping_fee) OVER w_part AS status_stddev_samp_shipping_fee,
        LAST_VALUE(shipping_fee) IGNORE NULLS OVER (PARTITION BY store_id ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_shipping_fee
    FROM valid
    WINDOW w_part AS (PARTITION BY status, store_id)
),
with_dims AS (
    -- Attach stores attributes
    SELECT
        a.*,
        p.store_name,
        p.city,
        p.region
    FROM scored a
    LEFT JOIN stores p
        ON a.store_id = p.id
),
enriched AS (
    -- Attach stores attributes
    SELECT
        e.*,
        b.store_name AS store_store_name,
        b.city AS store_city,
        b.opened_date
    FROM with_dims e
    LEFT JOIN stores b
        ON e.store_id = b.id
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CONCAT_WS(',', city, region) AS city_region_key,
        LAST_DAY(opened_date) AS opened_date_month_end
    FROM enriched
    WHERE amount >= 1260
),
with_dims_orders AS (
    -- Attach stores attributes
    SELECT
        a.*,
        d.opened_date AS store_opened_date,
        d.store_name AS store_store_name_2,
        d.city AS store_city_2
    FROM base a
    INNER JOIN stores d
        ON a.store_id = d.id
),
with_dims_orders_2 AS (
    -- Attach stores attributes
    SELECT
        e.*,
        b.city AS store_city_3,
        b.sqft,
        b.region AS store_region
    FROM with_dims_orders e
    LEFT JOIN stores b
        ON e.store_id = b.id
),
grouped AS (
    -- Multi-level aggregation
    SELECT
        DATE_TRUNC('QUARTER', opened_date_month_end) AS quarter_start,
        PERCENTILE_APPROX(shipping_fee, ARRAY(0.25, 0.5, 0.75)) AS shipping_fee_quartiles,
        CORR(discount, shipping_fee) AS corr_discount_shipping_fee,
        SUM(CASE WHEN store_city_3 = 'Tokyo' THEN amount ELSE 0 END) AS tokyo_amount
    FROM with_dims_orders_2
    GROUP BY DATE_TRUNC('QUARTER', opened_date_month_end)
)
SELECT
    quarter_start,
    shipping_fee_quartiles,
    corr_discount_shipping_fee,
    tokyo_amount
FROM grouped
ORDER BY tokyo_amount NULLS LAST;
