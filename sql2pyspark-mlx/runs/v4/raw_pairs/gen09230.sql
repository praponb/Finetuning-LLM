WITH scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        ROW_NUMBER() OVER (PARTITION BY browser ORDER BY started_at, id) AS row_seq,
        LAST_VALUE(country) IGNORE NULLS OVER w_ordered AS last_known_country,
        LAST_VALUE(country, TRUE) OVER (ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_country_2,
        FIRST_VALUE(country) IGNORE NULLS OVER (PARTITION BY customer_id ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_country,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY started_at DESC, id) AS seq_no
    FROM sessions
    WINDOW w_ordered AS (PARTITION BY device ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *, RANK() OVER (PARTITION BY country ORDER BY started_at DESC NULLS LAST) AS dedup_rank
    FROM scored
    QUALIFY dedup_rank = 1
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        started_at,
        country,
        row_seq,
        last_known_country,
        CASE WHEN country IN ('JP', 'UK') THEN 'Priority' ELSE 'Other' END AS country_group,
        COALESCE(country, 'N/A') AS country_clean
    FROM ranked
    WHERE started_at >= TIMESTAMP '2023-12-21 09:00:00'
      AND country IS NOT NULL
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        NTILE(4) OVER (ORDER BY row_seq DESC, id) AS row_seq_bucket,
        AVG(row_seq) OVER (ORDER BY row_seq, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS row_seq_avg_5_prior
    FROM filtered
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('QUARTER', started_at) AS quarter_start,
    ROUND(SUM(row_seq_avg_5_prior) / NULLIF(COUNT(CASE WHEN country = 'SG' THEN 1 END), 0), 2) AS row_seq_avg_5_prior_per_sg,
    COUNT(*) FILTER (WHERE country = 'US') AS us_rows
FROM enriched
GROUP BY quarter_start;
