WITH metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        channel,
        promo_code,
        ends_on,
        discount_pct,
        LEAD(discount_pct, 1) OVER (PARTITION BY promo_code, channel ORDER BY discount_pct, id) AS next_discount_pct,
        DENSE_RANK() OVER (PARTITION BY promo_code ORDER BY discount_pct DESC) AS discount_pct_dense_rank,
        AVG(discount_pct) OVER (PARTITION BY promo_code ORDER BY ends_on, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS discount_pct_avg_2,
        ROW_NUMBER() OVER w_ordered AS event_seq,
        SUM(discount_pct) OVER w_recent AS running_discount_pct
    FROM promotions
    WINDOW w_ordered AS (PARTITION BY promo_code, channel ORDER BY ends_on, id), w_recent AS (ORDER BY starts_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        discount_pct_avg_2 * discount_pct_dense_rank AS discount_pct_avg_2_times_discount_pct_dense_rank,
        COALESCE(channel, 'DIRECT') AS channel_clean
    FROM metrics
    WHERE ends_on > TO_DATE('2023-06-02')
      AND channel IS NOT NULL
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(event_seq) OVER (PARTITION BY channel) AS channel_sum_event_seq,
        FIRST_VALUE(running_discount_pct) IGNORE NULLS OVER (ORDER BY discount_pct, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_running_discount_pct
    FROM cleaned
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM scored s
    LEFT JOIN promotions pr
        ON s.channel = pr.channel
       AND s.ends_on >= pr.starts_on
       AND s.ends_on < pr.ends_on + INTERVAL '30' DAY
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY channel_sum_event_seq DESC NULLS FIRST, id, promo_id) AS rn
        FROM matched
    ) x
    WHERE rn = 1
)
-- Native SQL UNPIVOT (wide to long)
SELECT
    id,
    promo_id,
    promo_code,
    kpi,
    metric_value
FROM (
    SELECT
        id,
        promo_id,
        promo_code,
        next_discount_pct,
        discount_pct_avg_2_times_discount_pct_dense_rank
    FROM ranked
) src
UNPIVOT (
    metric_value FOR kpi IN (next_discount_pct, discount_pct_avg_2_times_discount_pct_dense_rank)
);
