WITH scored AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY ends_on ASC, id ASC) AS seq_no,
        AVG(discount_pct) OVER (PARTITION BY promo_code ORDER BY discount_pct ASC, id ASC ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS discount_pct_avg_6_prior,
        AVG(discount_pct) OVER (PARTITION BY promo_code ORDER BY starts_on ASC, id ASC ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS discount_pct_avg_5_prior,
        LAST_VALUE(channel, TRUE) OVER (PARTITION BY promo_code ORDER BY starts_on ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_channel
    FROM promotions
),
latest AS (
    SELECT
        id,
        promo_code,
        seq_no,
        discount_pct_avg_6_prior,
        last_known_channel
    FROM scored
    QUALIFY ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY discount_pct DESC, id ASC) = 1
),
scored_promotions AS (
    SELECT
        *,
        AVG(seq_no) OVER w_grp AS running_avg_seq_no,
        MIN(seq_no) OVER w_grp AS running_min_seq_no,
        COUNT(seq_no) OVER w_grp AS running_count_seq_no
    FROM latest
    WINDOW w_grp AS (PARTITION BY promo_code ORDER BY seq_no ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
latest_promotions AS (
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY promo_code ORDER BY running_avg_seq_no DESC NULLS LAST) AS latest_rank
        FROM scored_promotions
    ) x
    WHERE latest_rank = 1
),
cleaned AS (
    SELECT
        *,
        LEAST(latest_rank, discount_pct_avg_6_prior) AS least_latest_rank_discount_pct_avg_6_prior,
        CAST(running_min_seq_no AS INT) AS running_min_seq_no_int
    FROM latest_promotions
    WHERE running_count_seq_no IS NOT NULL
),
ranked AS (
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY promo_code ORDER BY running_min_seq_no_int DESC NULLS FIRST) AS rank_in_group
        FROM cleaned
    ) ranked
    WHERE rank_in_group = 1
)
SELECT
    id,
    seq_no,
    discount_pct_avg_6_prior,
    last_known_channel,
    running_avg_seq_no,
    running_min_seq_no,
    latest_rank,
    least_latest_rank_discount_pct_avg_6_prior,
    running_min_seq_no_int
FROM ranked
ORDER BY seq_no DESC NULLS FIRST, discount_pct_avg_6_prior ASC;
