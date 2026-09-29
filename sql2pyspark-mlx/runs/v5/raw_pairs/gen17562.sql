WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        EXPLODE(coupon_codes) AS element
    FROM carts
),
joined AS (
    -- Attach customers attributes
    SELECT
        s.*,
        d.credit_limit,
        d.city
    FROM flattened s
    LEFT JOIN customers d
        ON s.customer_id = d.id
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY credit_limit DESC) AS credit_limit_cume_dist,
        COUNT(credit_limit) OVER (PARTITION BY element) AS element_count_credit_limit
    FROM joined
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        credit_limit_cume_dist - element_count_credit_limit AS credit_limit_cume_dist_minus_element_count_credit_limit,
        COALESCE(element, 'NONE') AS resolved_element
    FROM with_windows
    WHERE city IS NOT NULL
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        customer_id,
        credit_limit,
        city,
        credit_limit_cume_dist,
        element_count_credit_limit,
        credit_limit_cume_dist_minus_element_count_credit_limit,
        resolved_element,
        RANK() OVER (PARTITION BY element, city ORDER BY credit_limit DESC NULLS FIRST) AS credit_limit_rank,
        RANK() OVER (PARTITION BY city ORDER BY element_count_credit_limit ASC NULLS FIRST) AS element_count_credit_limit_rank,
        PERCENT_RANK() OVER (PARTITION BY element ORDER BY credit_limit_cume_dist DESC) AS credit_limit_cume_dist_percent_rank,
        PERCENT_RANK() OVER (PARTITION BY customer_id, city ORDER BY credit_limit DESC NULLS LAST) AS credit_limit_percent_rank
    FROM filtered
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        DENSE_RANK() OVER w_ordered AS element_count_credit_limit_rank_dense_rank,
        CUME_DIST() OVER (ORDER BY credit_limit_percent_rank DESC NULLS LAST) AS credit_limit_percent_rank_cume_dist
    FROM enriched
    WINDOW w_ordered AS (PARTITION BY customer_id ORDER BY element_count_credit_limit_rank ASC NULLS LAST)
),
enriched_carts AS (
    -- Attach customers attributes
    SELECT
        e.customer_id,
        e.credit_limit,
        e.city,
        e.credit_limit_cume_dist,
        e.element_count_credit_limit,
        e.credit_limit_percent_rank,
        e.element_count_credit_limit_rank_dense_rank,
        b.signup_date,
        b.segment,
        b.credit_limit AS customer_credit_limit
    FROM metrics e
    LEFT JOIN customers b
        ON e.customer_id = b.id
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        customer_id,
        customer_credit_limit,
        credit_limit_percent_rank,
        credit_limit
    FROM enriched_carts
) src
UNPIVOT (
    value FOR kpi IN (customer_credit_limit, credit_limit_percent_rank, credit_limit)
)
