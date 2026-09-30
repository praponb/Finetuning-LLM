WITH enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY started_at, id) seq_no,
        ROW_NUMBER() OVER (PARTITION BY browser ORDER BY started_at, id) row_seq,
        FIRST_VALUE(browser) IGNORE NULLS OVER w_ordered first_known_browser,
        ROW_NUMBER() OVER w_running seq_no_2,
        ROW_NUMBER() OVER (PARTITION BY device, country ORDER BY started_at, id) seq_no_3
    FROM sessions
    WINDOW w_ordered AS (PARTITION BY country, browser ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_running AS (PARTITION BY browser ORDER BY started_at DESC, id)
),
joined AS (
    -- Attach customers attributes
    SELECT
        a.*,
        p.city,
        p.signup_date,
        p.name
    FROM enriched a
    INNER JOIN customers p
        ON a.customer_id = p.id
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM joined s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events x
            WHERE x.session_id = s.id
              AND x.session_id IN (14, 20, 22, 26)
        )
),
matched_keys AS (
    -- Set difference on keys
    SELECT country
    FROM eligible
    EXCEPT DISTINCT
    SELECT country
    FROM sessions
    WHERE country IN ('SG', 'DE', 'JP')
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM eligible s
    INNER JOIN matched_keys k ON s.country = k.country
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        seq_no,
        row_seq,
        first_known_browser,
        seq_no_2,
        seq_no_3,
        city,
        signup_date,
        name,
        CUME_DIST() OVER (PARTITION BY device ORDER BY seq_no DESC NULLS LAST) seq_no_cume_dist,
        RANK() OVER (ORDER BY seq_no NULLS FIRST) seq_no_rank
    FROM verified
),
enriched_sessions AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(seq_no_2 / SUM(seq_no_2) OVER (PARTITION BY customer_id), 4) seq_no_2_share,
        DENSE_RANK() OVER (ORDER BY seq_no_3) seq_no_3_dense_rank,
        AVG(seq_no_cume_dist) OVER (ORDER BY CAST(started_at AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) seq_no_cume_dist_avg_7d,
        AVG(seq_no) OVER (ORDER BY CAST(started_at AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) seq_no_avg_30d,
        PERCENT_RANK() OVER (ORDER BY seq_no_2 NULLS LAST) seq_no_2_percent_rank
    FROM metrics
)
SELECT
    id,
    customer_id,
    browser,
    seq_no,
    row_seq,
    first_known_browser,
    seq_no_2,
    seq_no_3,
    signup_date
FROM enriched_sessions
ORDER BY seq_no NULLS FIRST, customer_id DESC
