WITH enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        LAST_VALUE(browser) IGNORE NULLS OVER (PARTITION BY customer_id ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_browser,
        FIRST_VALUE(browser, TRUE) OVER (PARTITION BY country ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_browser,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY started_at DESC, id) AS row_seq
    FROM sessions
),
enriched_sessions AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        FIRST_VALUE(row_seq, TRUE) OVER (PARTITION BY customer_id ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_row_seq,
        ROUND(row_seq / SUM(row_seq) OVER (PARTITION BY country), 4) AS row_seq_share,
        ROW_NUMBER() OVER w_part AS seq_no,
        SUM(row_seq) OVER w_recent AS row_seq_sum_30d
    FROM enriched
    WINDOW w_part AS (ORDER BY started_at DESC, id), w_recent AS (PARTITION BY country ORDER BY CAST(started_at AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW)
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN device IN ('tablet', 'desktop') THEN 'Priority' ELSE 'Other' END AS device_group,
        COALESCE(browser, 'unassigned') AS browser_filled,
        COALESCE(browser, 'N/A') AS browser_clean
    FROM enriched_sessions
    WHERE started_at > DATE '2023-07-13'
      AND browser IS NOT NULL
      AND country IN ('DE', 'US', 'JP')
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, RANK() OVER (PARTITION BY device ORDER BY row_seq DESC NULLS LAST) AS dedup_rank
    FROM base
    QUALIFY dedup_rank = 1
)
-- Grouped metrics
SELECT
    DATE_TRUNC('YEAR', started_at) AS year_start,
    CASE WHEN row_seq_sum_30d >= 40 THEN 'PREMIUM' ELSE 'STANDARD' END AS row_seq_sum_30d_tier,
    MAX(first_known_row_seq) AS max_first_known_row_seq,
    COUNT(DISTINCT customer_id) AS distinct_customer_ids,
    MAX(started_at) AS last_started_at,
    ROUND(SUM(seq_no) / NULLIF(COUNT(CASE WHEN country = 'DE' THEN 1 END), 0), 2) AS seq_no_per_de,
    SUM(CASE WHEN country = 'JP' THEN seq_no ELSE 0 END) AS jp_seq_no
FROM deduped
GROUP BY ROLLUP (DATE_TRUNC('YEAR', started_at), CASE WHEN row_seq_sum_30d >= 40 THEN 'PREMIUM' ELSE 'STANDARD' END);
