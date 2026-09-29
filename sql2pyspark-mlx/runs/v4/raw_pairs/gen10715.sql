WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        COALESCE(age, 0) AS age_filled,
        ROUND(age, 0) AS age_rounded,
        COALESCE(segment, 'N/A') AS segment_clean
    FROM customers
    WHERE segment IN ('SMB', 'Corporate')
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROUND(credit_limit / SUM(credit_limit) OVER (PARTITION BY segment), 4) AS credit_limit_share,
        ROUND(credit_limit / SUM(credit_limit) OVER (PARTITION BY city), 4) AS credit_limit_share_2,
        MAX(credit_limit) OVER (PARTITION BY segment) AS segment_max_credit_limit
    FROM cleaned
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY signup_date DESC NULLS LAST, id ASC) AS row_num
        FROM enriched
    ) x
    WHERE row_num = 1
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY age DESC NULLS FIRST, id ASC) AS dedup_rank
    FROM deduped
    QUALIFY dedup_rank = 1
),
stats AS (
    -- Multi-level aggregation
    SELECT
        segment,
        APPROX_PERCENTILE(credit_limit_share, ARRAY(0.25, 0.5, 0.75)) AS credit_limit_share_quartiles,
        COUNT(*) FILTER (WHERE country = 'UK') AS uk_rows,
        STDDEV_SAMP(credit_limit) AS credit_limit_stddev
    FROM ranked
    GROUP BY segment
    HAVING uk_rows > 1
)
SELECT
    segment,
    credit_limit_share_quartiles,
    uk_rows,
    credit_limit_stddev
FROM stats
ORDER BY credit_limit_stddev DESC NULLS LAST
