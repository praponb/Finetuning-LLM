WITH with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        channel,
        starts_on,
        discount_pct,
        AVG(discount_pct) OVER (PARTITION BY channel ORDER BY starts_on, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) discount_pct_avg_6_prior,
        NTILE(3) OVER (PARTITION BY promo_code, channel ORDER BY discount_pct, id) discount_pct_bucket
    FROM promotions
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM with_windows src
    LEFT JOIN promotions pr
        ON src.channel = pr.channel
       AND src.starts_on >= pr.starts_on
       AND src.starts_on <= DATE_ADD(pr.ends_on, 7)
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        channel,
        starts_on,
        discount_pct,
        discount_pct_avg_6_prior,
        promo_id,
        promo_code,
        promotion_discount_pct
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY starts_on, id, promo_id) AS rn
        FROM matched
    ) t
    WHERE rn = 1
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY discount_pct_avg_6_prior DESC, id, promo_id) AS rank_in_group
        FROM ranked
    ) t
    WHERE rank_in_group = 1
),
deduped_promotions AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY discount_pct DESC, id, promo_id) AS dedup_rank
    FROM deduped
    QUALIFY dedup_rank = 1
),
matched_keys AS (
    -- Set difference on keys
    SELECT id
    FROM deduped_promotions
    EXCEPT ALL
    SELECT id
    FROM promotions
    WHERE discount_pct BETWEEN 0.2 AND 0.26
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM deduped_promotions s
    INNER JOIN matched_keys k ON s.id = k.id
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        ROUND(discount_pct / SUM(discount_pct) OVER (PARTITION BY promo_code), 4) discount_pct_share,
        RANK() OVER (PARTITION BY promo_code ORDER BY discount_pct_avg_6_prior DESC NULLS LAST) discount_pct_avg_6_prior_rank,
        PERCENT_RANK() OVER (PARTITION BY channel, promo_code ORDER BY discount_pct_avg_6_prior DESC NULLS FIRST) discount_pct_avg_6_prior_percent_rank,
        CUME_DIST() OVER (PARTITION BY channel ORDER BY promo_id NULLS FIRST) promo_id_cume_dist
    FROM reconciled
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN promo_id_cume_dist >= 0.76 THEN 'HIGH' ELSE 'LOW' END AS promo_id_cume_dist_tier,
        promo_id,
        COUNT_IF(channel = 'partner') AS partner_events,
        COUNT(DISTINCT id) AS distinct_ids,
        PERCENTILE_APPROX(discount_pct, 0.95) AS discount_pct_p95,
        GROUPING(CASE WHEN promo_id_cume_dist >= 0.76 THEN 'HIGH' ELSE 'LOW' END) AS gid
    FROM scored
    GROUP BY GROUPING SETS ((CASE WHEN promo_id_cume_dist >= 0.76 THEN 'HIGH' ELSE 'LOW' END, promo_id), (CASE WHEN promo_id_cume_dist >= 0.76 THEN 'HIGH' ELSE 'LOW' END), ())
)
SELECT partner_events, discount_pct_p95
FROM grouped
ORDER BY discount_pct_p95 DESC
