WITH metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(quantity) OVER w_grp AS running_quantity,
        AVG(unit_price) OVER w_part AS unit_price_avg_5_prior
    FROM order_items
    WINDOW w_grp AS (PARTITION BY product_id ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_part AS (ORDER BY quantity, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING)
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY unit_price_avg_5_prior DESC NULLS FIRST, id) AS latest_rank
        FROM metrics
    ) ranked
    WHERE latest_rank = 1
),
joined AS (
    -- Attach orders attributes
    SELECT
        src.*,
        p.order_date,
        p.amount
    FROM deduped src
    LEFT JOIN orders p
        ON src.order_id = p.id
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM joined
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY latest_rank DESC, id) = 1
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(unit_price_avg_5_prior, TRUE) OVER (PARTITION BY product_id ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_unit_price_avg_5_prior,
        SUM(amount) OVER (PARTITION BY product_id ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_amount
    FROM ranked
),
summary AS (
    -- Grouped metrics
    SELECT
        product_id,
        CASE WHEN cumulative_amount >= 6870 THEN 'LARGE' ELSE 'SMALL' END AS cumulative_amount_tier,
        AVG(quantity) AS avg_quantity,
        BIT_AND(quantity) AS quantity_bit_and,
        MIN(quantity) AS min_quantity
    FROM enriched
    GROUP BY product_id, cumulative_amount_tier
)
SELECT
    s.product_id,
    s.cumulative_amount_tier,
    s.avg_quantity,
    s.quantity_bit_and,
    s.min_quantity,
    (SELECT SUM(o2.quantity) FROM order_items o2 WHERE o2.product_id = s.product_id) AS sum_order_item_quantity
FROM summary s
ORDER BY avg_quantity;
