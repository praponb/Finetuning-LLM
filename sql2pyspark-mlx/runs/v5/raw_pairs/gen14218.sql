WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        DATE_FORMAT(ends_on, 'MMM yyyy') AS ends_on_label
    FROM promotions
    WHERE channel IS NOT NULL
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM filtered
    QUALIFY ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY starts_on, id) = 1
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CONCAT(channel, ' - ', promo_code) AS channel_promo_code,
        COALESCE(channel, 'OTHER') AS resolved_channel
    FROM ranked
    WHERE MONTH(starts_on) IN (1, 6, 9)
),
scored AS (
    -- Ranking and gap features
    SELECT *, LAST_VALUE(channel, TRUE) OVER (ORDER BY starts_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_channel
    FROM base
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        channel,
        starts_on,
        ends_on,
        ends_on_label,
        ROW_NUMBER() OVER (ORDER BY ends_on, id) AS seq_no
    FROM scored
)
-- Grouped metrics
SELECT
    seq_no,
    MIN(seq_no) AS min_seq_no,
    COUNT(*) FILTER (WHERE channel = 'web') AS web_rows
FROM metrics
GROUP BY seq_no
ORDER BY min_seq_no NULLS LAST;
