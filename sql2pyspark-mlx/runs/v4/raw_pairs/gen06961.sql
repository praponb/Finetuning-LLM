WITH scored AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY promo_code, channel ORDER BY ends_on DESC, id) AS event_seq,
        CUME_DIST() OVER (PARTITION BY promo_code ORDER BY discount_pct DESC) AS discount_pct_cume_dist,
        ROW_NUMBER() OVER (PARTITION BY channel, promo_code ORDER BY ends_on DESC, id) AS event_seq_2
    FROM promotions
),
remaining_keys AS (
    SELECT id, channel
    FROM scored
    EXCEPT ALL
    SELECT id, channel
    FROM promotions
    WHERE channel = 'mobile'
),
reconciled AS (
    SELECT s.*
    FROM scored s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.channel = k.channel
),
with_windows AS (
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        event_seq,
        discount_pct_cume_dist,
        event_seq_2,
        CUME_DIST() OVER (PARTITION BY promo_code ORDER BY event_seq_2 DESC NULLS LAST) AS event_seq_2_cume_dist,
        AVG(event_seq_2) OVER (PARTITION BY promo_code) AS promo_code_avg_event_seq_2
    FROM reconciled
),
with_window AS (
    SELECT
        a.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM with_windows a
    LEFT JOIN promotions pr
        ON a.channel = pr.channel
       AND a.starts_on >= pr.starts_on
       AND a.starts_on < pr.ends_on + INTERVAL 30 DAYS
),
summary AS (
    SELECT
        channel,
        DATE_TRUNC('WEEK', ends_on) AS week_start,
        ROUND(AVG(event_seq_2), 2) AS avg_event_seq_2,
        AVG(discount_pct_cume_dist) AS avg_discount_pct_cume_dist,
        COUNT(*) FILTER (WHERE channel = 'mobile') AS mobile_rows,
        APPROX_PERCENTILE(discount_pct_cume_dist, 0.5) AS discount_pct_cume_dist_p50,
        ROUND(AVG(promo_code_avg_event_seq_2), 2) AS avg_promo_code_avg_event_seq_2,
        SIZE(COLLECT_SET(channel)) AS num_channels,
        DENSE_RANK() OVER (PARTITION BY channel ORDER BY SIZE(COLLECT_SET(channel)) DESC) AS num_channels_dense_rank
    FROM with_window
    GROUP BY channel, week_start
    HAVING avg_promo_code_avg_event_seq_2 > 0
)
SELECT
    channel,
    week_start,
    avg_discount_pct_cume_dist,
    mobile_rows,
    discount_pct_cume_dist_p50,
    avg_promo_code_avg_event_seq_2,
    num_channels,
    num_channels_dense_rank
FROM summary
ORDER BY num_channels DESC NULLS FIRST
