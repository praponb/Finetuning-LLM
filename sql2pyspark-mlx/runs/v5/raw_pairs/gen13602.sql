WITH enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY discount_pct DESC) AS discount_pct_cume_dist,
        ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY starts_on, id) AS seq_no,
        LAG(discount_pct, 2) OVER (PARTITION BY channel ORDER BY starts_on, id) AS prev_discount_pct,
        LAST_VALUE(channel) IGNORE NULLS OVER (PARTITION BY channel ORDER BY starts_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_channel
    FROM promotions
    WHERE discount_pct >= 0.23
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY promo_code, channel ORDER BY discount_pct_cume_dist NULLS FIRST) AS rn
        FROM enriched
    ) ranked
    WHERE rn = 1
),
enriched_promotions AS (
    -- Ranking and gap features
    SELECT
        id,
        channel,
        starts_on,
        ends_on,
        discount_pct,
        seq_no,
        last_known_channel,
        rn,
        ROW_NUMBER() OVER (PARTITION BY channel ORDER BY ends_on DESC, id) AS seq_no_2,
        AVG(discount_pct) OVER (PARTITION BY promo_code, channel ORDER BY discount_pct_cume_dist, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS discount_pct_avg_3_prior
    FROM deduped
),
enriched_promotions_2 AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY channel ORDER BY discount_pct DESC) AS discount_pct_percent_rank,
        AVG(seq_no_2) OVER (PARTITION BY channel ORDER BY discount_pct_avg_3_prior, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS seq_no_2_avg_6,
        AVG(seq_no_2) OVER (PARTITION BY channel ORDER BY rn, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS seq_no_2_avg_5
    FROM enriched_promotions
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        channel,
        starts_on,
        ends_on,
        discount_pct,
        seq_no,
        rn,
        seq_no_2,
        discount_pct_avg_3_prior,
        discount_pct_percent_rank,
        seq_no_2_avg_6,
        CASE WHEN seq_no_2_avg_5 >= 8 THEN 'High' WHEN seq_no_2_avg_5 >= 4 THEN 'Medium' ELSE 'Low' END AS seq_no_2_avg_5_band
    FROM enriched_promotions_2
    WHERE seq_no BETWEEN 5 AND 6
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY discount_pct_percent_rank DESC NULLS FIRST, id) AS rank_in_group
        FROM cleaned
    ) x
    WHERE rank_in_group = 1
)
SELECT
    id,
    starts_on,
    ends_on,
    discount_pct,
    seq_no,
    rn,
    seq_no_2,
    discount_pct_avg_3_prior,
    discount_pct_percent_rank
FROM ranked
ORDER BY id DESC NULLS LAST
