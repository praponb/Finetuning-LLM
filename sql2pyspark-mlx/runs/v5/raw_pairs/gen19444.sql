WITH with_dims AS (
    -- Attach customers attributes
    SELECT
        e.*,
        c.credit_limit,
        c.segment
    FROM accounts e
    LEFT JOIN customers c
        ON e.customer_id <=> c.id
),
enriched AS (
    -- Attach customers attributes
    SELECT
        e.id,
        e.customer_id,
        e.opened_date,
        e.status,
        e.credit_limit,
        p.segment AS customer_segment,
        p.name,
        p.age
    FROM with_dims e
    LEFT JOIN customers p
        ON e.customer_id <=> p.id
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY age DESC NULLS FIRST, id) AS rank_in_group
    FROM enriched
    QUALIFY rank_in_group = 1
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        opened_date,
        status,
        credit_limit,
        customer_segment,
        age,
        rank_in_group,
        credit_limit + rank_in_group AS credit_limit_plus_rank_in_group
    FROM deduped
    WHERE opened_date >= TO_DATE('2023-07-13')
      AND customer_segment IS NOT NULL
),
with_last AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.status,
        s.credit_limit,
        s.customer_segment,
        s.age,
        s.rank_in_group,
        top1.amount
    FROM filtered s
    JOIN LATERAL (
            SELECT c.amount
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_time <= s.opened_date
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN credit_limit >= 14100 THEN 'PREMIUM' ELSE 'STANDARD' END AS credit_limit_tier,
    COUNT(DISTINCT customer_segment) AS distinct_customer_segments,
    SORT_ARRAY(COLLECT_SET(customer_segment)) AS customer_segment_list,
    ROUND(AVG(credit_limit), 2) AS avg_credit_limit
FROM with_last
GROUP BY 1
ORDER BY distinct_customer_segments
