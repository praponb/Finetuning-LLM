WITH deduped AS (
    SELECT *
    FROM promotions
    QUALIFY RANK() OVER (PARTITION BY channel ORDER BY starts_on) = 1
),
scored AS (
    SELECT
        id,
        channel,
        LEAD(discount_pct) OVER w_grp AS next_discount_pct,
        LAG(discount_pct) OVER w_grp AS prev_discount_pct,
        ROW_NUMBER() OVER w_grp AS event_seq,
        LEAD(discount_pct, 1) OVER (PARTITION BY promo_code ORDER BY ends_on, id) AS next_discount_pct_2
    FROM deduped
    WINDOW w_grp AS (PARTITION BY channel ORDER BY ends_on, id)
),
with_windows AS (
    SELECT
        id,
        event_seq,
        next_discount_pct_2,
        ROW_NUMBER() OVER w_part AS event_seq_2,
        SUM(prev_discount_pct) OVER (PARTITION BY channel ORDER BY next_discount_pct_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_prev_discount_pct
    FROM scored
    WINDOW w_part AS (PARTITION BY channel ORDER BY prev_discount_pct, id)
),
scored_promotions AS (
    SELECT
        *,
        MAX(next_discount_pct_2) OVER w_recent AS running_max_next_discount_pct_2,
        COUNT(next_discount_pct_2) OVER w_recent AS running_count_next_discount_pct_2,
        AVG(next_discount_pct_2) OVER w_recent AS running_avg_next_discount_pct_2,
        MIN(next_discount_pct_2) OVER w_recent AS running_min_next_discount_pct_2,
        FIRST_VALUE(event_seq_2, TRUE) OVER (ORDER BY event_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_seq_2
    FROM with_windows
    WINDOW w_recent AS (ORDER BY next_discount_pct_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
base AS (
    SELECT
        id,
        next_discount_pct_2,
        event_seq_2,
        running_max_next_discount_pct_2,
        running_count_next_discount_pct_2,
        running_avg_next_discount_pct_2,
        running_min_next_discount_pct_2,
        first_known_event_seq_2,
        ROUND(event_seq / next_discount_pct_2, 2) AS event_seq_per_next_discount_pct_2
    FROM scored_promotions
    WHERE running_max_next_discount_pct_2 < 0.17
),
metrics AS (
    SELECT
        id,
        event_seq_2,
        running_max_next_discount_pct_2,
        running_avg_next_discount_pct_2,
        SUM(running_max_next_discount_pct_2) OVER w_recent AS running_sum_running_max_next_discount_pct_2,
        MAX(running_max_next_discount_pct_2) OVER w_recent AS running_max_running_max_next_discount_pct_2,
        AVG(running_max_next_discount_pct_2) OVER w_recent AS running_avg_running_max_next_discount_pct_2,
        COUNT(running_max_next_discount_pct_2) OVER w_recent AS running_count_running_max_next_discount_pct_2
    FROM base
    WINDOW w_recent AS (ORDER BY event_seq_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    running_max_next_discount_pct_2,
    running_avg_next_discount_pct_2,
    running_sum_running_max_next_discount_pct_2,
    running_count_running_max_next_discount_pct_2
FROM metrics
ORDER BY running_max_next_discount_pct_2 NULLS LAST
