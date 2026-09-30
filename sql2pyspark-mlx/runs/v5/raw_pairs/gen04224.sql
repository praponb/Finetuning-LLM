WITH with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        channel,
        ends_on,
        ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY discount_pct, id) AS seq_no,
        SUM(discount_pct) OVER (PARTITION BY channel, promo_code ORDER BY ends_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_discount_pct
    FROM promotions
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        channel,
        cumulative_discount_pct
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY seq_no NULLS LAST, id) AS rank_in_group
        FROM with_windows
    ) x
    WHERE rank_in_group = 1
),
kept_keys AS (
    -- Set difference on keys
    SELECT id, channel
    FROM latest
    EXCEPT
    SELECT id, channel
    FROM promotions
    WHERE starts_on BETWEEN '2024-01-27' AND '2024-08-18'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM latest s
    JOIN kept_keys k ON s.id = k.id AND s.channel = k.channel
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, LENGTH(channel) AS channel_length
    FROM reconciled
    WHERE channel IS NOT NULL
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        cumulative_discount_pct * channel_length AS cumulative_discount_pct_times_channel_length,
        COALESCE(channel, 'DIRECT') AS resolved_channel
    FROM cleaned
    WHERE channel = 'web'
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        RANK() OVER (PARTITION BY channel ORDER BY cumulative_discount_pct) AS cumulative_discount_pct_rank,
        PERCENT_RANK() OVER (ORDER BY cumulative_discount_pct_times_channel_length DESC NULLS FIRST) AS cumulative_discount_pct_times_channel_length_percent_rank
    FROM filtered
    WHERE cumulative_discount_pct < 1
),
with_windows_promotions AS (
    -- Ranking and gap features
    SELECT
        id,
        channel_length,
        cumulative_discount_pct_times_channel_length,
        cumulative_discount_pct_rank,
        COUNT(cumulative_discount_pct_times_channel_length) OVER (PARTITION BY channel) AS channel_count_cumulative_discount_pct_times_channel_length,
        DENSE_RANK() OVER w_hist AS cumulative_discount_pct_times_channel_length_percent_rank_dense_rank
    FROM scored
    WINDOW w_hist AS (PARTITION BY channel ORDER BY cumulative_discount_pct_times_channel_length_percent_rank DESC NULLS FIRST)
),
summary AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN cumulative_discount_pct_times_channel_length_percent_rank_dense_rank >= 7 THEN 'HIGH' ELSE 'LOW' END AS cumulative_discount_pct_times_channel_length_percent_rank_dense_rank_tier,
        CASE WHEN channel_count_cumulative_discount_pct_times_channel_length >= 15 THEN 'LARGE' ELSE 'SMALL' END AS channel_count_cumulative_discount_pct_times_channel_length_tier,
        COUNT(*) AS event_count,
        APPROX_PERCENTILE(cumulative_discount_pct_times_channel_length, ARRAY(0.25, 0.5, 0.75)) AS cumulative_discount_pct_times_channel_length_quartiles,
        COUNT(DISTINCT id) AS distinct_ids
    FROM with_windows_promotions
    GROUP BY CASE WHEN cumulative_discount_pct_times_channel_length_percent_rank_dense_rank >= 7 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN channel_count_cumulative_discount_pct_times_channel_length >= 15 THEN 'LARGE' ELSE 'SMALL' END
)
SELECT
    cumulative_discount_pct_times_channel_length_percent_rank_dense_rank_tier,
    channel_count_cumulative_discount_pct_times_channel_length_tier,
    cumulative_discount_pct_times_channel_length_quartiles,
    distinct_ids
FROM summary
ORDER BY cumulative_discount_pct_times_channel_length_percent_rank_dense_rank_tier DESC NULLS FIRST;
