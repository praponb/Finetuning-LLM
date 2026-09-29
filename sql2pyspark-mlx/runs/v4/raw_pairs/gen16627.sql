WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        elem.product_id,
        elem.qty,
        elem.price,
        elem.promo
    FROM (
        SELECT
            id,
            customer_id,
            created_at,
            EXPLODE(items) AS elem
        FROM carts
    ) e
),
base AS (
    -- Keep recent, valid rows
    SELECT
        customer_id,
        created_at,
        product_id,
        price,
        HOUR(created_at) created_at_hour,
        ROUND(qty * 0.85, 2) qty_adjusted
    FROM lines
    WHERE created_at >= TIMESTAMP '2025-03-01 18:00:00'
      AND promo IS NOT NULL
),
base_carts AS (
    -- Basic cleansing and defaults
    SELECT
        customer_id,
        created_at,
        price,
        created_at_hour,
        qty_adjusted,
        COALESCE(price, 0) price_filled
    FROM base
    WHERE created_at > DATE '2025-03-02'
      AND customer_id IS NOT NULL
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        customer_id,
        price,
        qty_adjusted,
        MONTH(created_at) created_at_month
    FROM base_carts
    WHERE created_at >= DATE '2025-03-02'
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY created_at_month DESC NULLS LAST) created_at_month_dense_rank,
        RANK() OVER (ORDER BY created_at_month DESC NULLS LAST) created_at_month_rank,
        STDDEV_SAMP(qty_adjusted) OVER (PARTITION BY customer_id) customer_id_stddev_samp_qty_adjusted
    FROM cleaned
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        src.*,
        b.country,
        b.email
    FROM scored src
    INNER JOIN customers b
        ON src.customer_id = b.id
)
SELECT
    customer_id,
    price,
    created_at_month,
    created_at_month_rank,
    country,
    email
FROM with_dims
ORDER BY country DESC
