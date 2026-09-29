WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        k,
        v
    FROM carts
    LATERAL VIEW EXPLODE(attributes) t AS k, v
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY created_at ASC, id ASC, k ASC) seq_no,
        ROW_NUMBER() OVER (ORDER BY created_at DESC, id ASC, k ASC) row_seq,
        ROW_NUMBER() OVER (PARTITION BY k ORDER BY created_at ASC, id ASC, k ASC) event_seq,
        FIRST_VALUE(k) IGNORE NULLS OVER w_hist first_known_k,
        FIRST_VALUE(k) IGNORE NULLS OVER w_grp first_known_k_2
    FROM lines
    WINDOW w_hist AS (PARTITION BY customer_id, k ORDER BY created_at ASC, id ASC, k ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_grp AS (PARTITION BY customer_id ORDER BY created_at ASC, id ASC, k ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
enriched AS (
    -- Attach customers attributes
    SELECT
        src.*,
        c.city,
        c.email
    FROM metrics src
    LEFT JOIN customers c
        ON src.customer_id = c.id
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        created_at,
        k,
        v,
        seq_no,
        row_seq,
        first_known_k,
        first_known_k_2,
        LEAST(seq_no, event_seq) least_seq_no_event_seq,
        COALESCE(k, 'OTHER') k_filled
    FROM enriched
    WHERE row_seq <> 8
)
SELECT
    s.id,
    s.customer_id,
    s.created_at,
    s.k,
    s.v,
    s.row_seq,
    s.first_known_k,
    s.first_known_k_2,
    s.least_seq_no_event_seq,
    CASE WHEN k RLIKE '^(gift|device)$' THEN 'PRIORITY' WHEN k RLIKE '^(utm_source|weight_kg)$' THEN 'OTHER_ZONE' ELSE 'GROUP_A' END AS k_zone,
    (SELECT AVG(x.balance) FROM accounts x WHERE x.customer_id = s.customer_id) AS avg_account_balance
FROM valid s
ORDER BY customer_id ASC
