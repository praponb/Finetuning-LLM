WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        DATE_ADD(ends_on, 83) AS ends_on_plus_83d,
        COALESCE(channel, 'UNKNOWN') AS channel_clean
    FROM promotions
    WHERE discount_pct < 0.18
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        promo_code,
        ends_on,
        discount_pct,
        channel_clean,
        LEAD(discount_pct, 1) OVER w_part AS next_discount_pct,
        ROW_NUMBER() OVER (ORDER BY starts_on, id) AS event_seq,
        LAST_VALUE(channel, TRUE) OVER (ORDER BY ends_on_plus_83d, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_channel
    FROM base
    WINDOW w_part AS (PARTITION BY channel ORDER BY discount_pct, id)
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT promo_code, id
    FROM metrics
    INTERSECT ALL
    SELECT promo_code, id
    FROM promotions
    WHERE promo_code NOT IN ('SPRING10', 'WELCOME5')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM metrics s
    INNER JOIN remaining_keys k ON s.promo_code = k.promo_code AND s.id = k.id
),
with_windows AS (
    -- Ranking and gap features
    SELECT *, AVG(discount_pct) OVER (PARTITION BY promo_code) AS promo_code_avg_discount_pct
    FROM kept_rows
    WHERE next_discount_pct <= 0.2
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        promo_code,
        discount_pct,
        next_discount_pct,
        last_known_channel,
        promo_code_avg_discount_pct,
        AVG(event_seq) OVER (PARTITION BY promo_code) AS promo_code_avg_event_seq,
        DENSE_RANK() OVER (ORDER BY next_discount_pct DESC NULLS LAST) AS next_discount_pct_dense_rank,
        ROUND(event_seq / SUM(event_seq) OVER (PARTITION BY promo_code), 4) AS event_seq_share,
        RANK() OVER (PARTITION BY promo_code ORDER BY promo_code_avg_discount_pct NULLS LAST) AS promo_code_avg_discount_pct_rank,
        COUNT(discount_pct) OVER (PARTITION BY promo_code) AS promo_code_count_discount_pct
    FROM with_windows
),
scored_promotions AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        promo_code_avg_discount_pct,
        next_discount_pct_dense_rank,
        ROUND(next_discount_pct / SUM(next_discount_pct) OVER (PARTITION BY promo_code), 4) AS next_discount_pct_share,
        RANK() OVER (ORDER BY next_discount_pct DESC NULLS LAST) AS next_discount_pct_rank,
        AVG(next_discount_pct) OVER w_running AS promo_code_avg_next_discount_pct
    FROM scored
    WINDOW w_running AS (PARTITION BY promo_code)
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        next_discount_pct_share - next_discount_pct_dense_rank AS next_discount_pct_share_minus_next_discount_pct_dense_rank,
        ROUND(next_discount_pct_share / next_discount_pct_rank, 2) AS next_discount_pct_share_per_next_discount_pct_rank
    FROM scored_promotions
    WHERE next_discount_pct_rank > 8
)
SELECT
    promo_code_avg_discount_pct,
    next_discount_pct_dense_rank,
    next_discount_pct_share_minus_next_discount_pct_dense_rank,
    next_discount_pct_share_per_next_discount_pct_rank
FROM filtered;
