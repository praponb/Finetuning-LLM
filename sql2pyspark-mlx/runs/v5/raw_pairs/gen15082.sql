WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN city IN ('Tokyo', 'Chiang Mai') THEN 'Core' ELSE 'Other' END AS city_group,
        COALESCE(city, 'OTHER') AS city_clean
    FROM customers
    WHERE credit_limit <= 12300
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        name,
        email,
        credit_limit,
        age,
        ROUND(credit_limit / SUM(credit_limit) OVER (PARTITION BY country, city), 4) AS credit_limit_share,
        ROUND(age / SUM(age) OVER (PARTITION BY city), 4) AS age_share,
        DENSE_RANK() OVER (ORDER BY credit_limit DESC NULLS FIRST) AS credit_limit_dense_rank
    FROM cleaned
    WHERE signup_date >= '2024-10-01'
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        name,
        credit_limit_share,
        LAST_VALUE(credit_limit_dense_rank, TRUE) OVER (ORDER BY credit_limit_share, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_credit_limit_dense_rank,
        LAST_VALUE(credit_limit_share, TRUE) OVER (ORDER BY credit_limit_share, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_credit_limit_share,
        SUM(credit_limit) OVER (ORDER BY age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_credit_limit,
        ROW_NUMBER() OVER (ORDER BY credit_limit_dense_rank DESC, id) AS event_seq
    FROM metrics
),
latest_match AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.status,
        latest.order_amount
    FROM enriched s
    JOIN LATERAL (
            SELECT c.status, c.amount AS order_amount
            FROM orders c
            WHERE c.customer_id = s.id
            ORDER BY c.shipping_fee DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM latest_match s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders ch
            WHERE ch.customer_id = s.id
              AND ch.customer_id IN (6, 13, 32, 34)
        )
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM not_flagged
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status ORDER BY credit_limit_share DESC, id) = 1
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN last_known_credit_limit_share >= 0.77 THEN 'PREMIUM' ELSE 'STANDARD' END AS last_known_credit_limit_share_tier,
    status,
    AVG(last_known_credit_limit_dense_rank) AS avg_last_known_credit_limit_dense_rank,
    SUM(CASE WHEN status = 'PENDING' THEN last_known_credit_limit_dense_rank ELSE 0 END) AS pending_last_known_credit_limit_dense_rank,
    AVG(last_known_credit_limit_dense_rank) AS avg_last_known_credit_limit_dense_rank_2,
    MAX(credit_limit_share) FILTER (WHERE status = 'COMPLETED') AS completed_max_credit_limit_share,
    COUNT(DISTINCT status) AS distinct_status
FROM ranked
GROUP BY GROUPING SETS ((CASE WHEN last_known_credit_limit_share >= 0.77 THEN 'PREMIUM' ELSE 'STANDARD' END, status), (CASE WHEN last_known_credit_limit_share >= 0.77 THEN 'PREMIUM' ELSE 'STANDARD' END), ());
