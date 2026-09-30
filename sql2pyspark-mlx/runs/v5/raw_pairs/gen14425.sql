WITH filtered AS (
    -- Keep recent, valid rows
    SELECT *, ADD_MONTHS(opened_date, 12) AS opened_date_plus_months
    FROM stores
    WHERE region IS DISTINCT FROM 'Central'
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY city, region ORDER BY opened_date_plus_months) AS row_num
        FROM filtered
    ) ranked
    WHERE row_num = 1
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(sqft, 1) OVER (ORDER BY sqft, id) AS next_sqft,
        FIRST_VALUE(sqft, TRUE) OVER (PARTITION BY region, city ORDER BY opened_date_plus_months, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_sqft,
        FIRST_VALUE(sqft) IGNORE NULLS OVER (ORDER BY opened_date_plus_months, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_sqft_2,
        ROW_NUMBER() OVER (PARTITION BY city, region ORDER BY opened_date_plus_months DESC, id) AS event_seq
    FROM deduped
    WHERE region IN ('South', 'West', 'North')
),
deduped_stores AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        opened_date_plus_months,
        next_sqft,
        first_known_sqft,
        event_seq
    FROM with_windows
    QUALIFY ROW_NUMBER() OVER (PARTITION BY city, region ORDER BY first_known_sqft, id) = 1
),
latest_match AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.shipping_fee,
        x.order_channel
    FROM deduped_stores s
    LEFT JOIN LATERAL (
            SELECT c.shipping_fee, c.channel AS order_channel
            FROM orders c
            WHERE c.store_id = s.id
            ORDER BY c.discount DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
SELECT
    id,
    opened_date_plus_months,
    next_sqft,
    shipping_fee,
    order_channel
FROM latest_match
ORDER BY next_sqft DESC NULLS FIRST;
