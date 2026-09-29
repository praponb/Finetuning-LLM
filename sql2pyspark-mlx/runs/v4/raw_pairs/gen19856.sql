WITH base AS (
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        YEAR(starts_on) AS starts_on_year
    FROM promotions
    WHERE ends_on > TO_DATE('2024-06-09')
      AND channel IS NOT NULL
),
valid AS (
    SELECT
        id,
        promo_code,
        starts_on,
        ends_on,
        starts_on_year,
        discount_pct + starts_on_year AS discount_pct_plus_starts_on_year
    FROM base
    WHERE ends_on > DATE '2023-10-21'
      AND starts_on BETWEEN '2024-02-01' AND '2024-07-01'
),
scored AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY ends_on DESC, id) AS row_seq,
        RANK() OVER w_recent AS starts_on_year_rank,
        SUM(starts_on_year) OVER (PARTITION BY promo_code ORDER BY starts_on_year, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_starts_on_year,
        MAX(starts_on_year) OVER (PARTITION BY promo_code) AS promo_code_max_starts_on_year,
        DENSE_RANK() OVER w_ordered AS starts_on_year_dense_rank
    FROM valid
    WINDOW w_recent AS (PARTITION BY promo_code ORDER BY starts_on_year DESC), w_ordered AS (ORDER BY starts_on_year DESC NULLS LAST)
),
scored_promotions AS (
    SELECT
        id,
        promo_code,
        starts_on,
        ends_on,
        starts_on_year,
        discount_pct_plus_starts_on_year,
        row_seq,
        starts_on_year_rank,
        cumulative_starts_on_year,
        promo_code_max_starts_on_year,
        starts_on_year_dense_rank,
        SUM(starts_on_year_rank) OVER (PARTITION BY promo_code ORDER BY starts_on_year_dense_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_starts_on_year_rank,
        SUM(cumulative_starts_on_year) OVER (PARTITION BY promo_code) AS promo_code_sum_cumulative_starts_on_year,
        FIRST_VALUE(starts_on_year) IGNORE NULLS OVER (PARTITION BY promo_code ORDER BY starts_on_year_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_starts_on_year,
        CUME_DIST() OVER (PARTITION BY promo_code ORDER BY cumulative_starts_on_year DESC NULLS LAST) AS cumulative_starts_on_year_cume_dist,
        SUM(starts_on_year_rank) OVER (PARTITION BY promo_code ORDER BY starts_on_year_dense_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_starts_on_year_rank
    FROM scored
),
latest AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY starts_on_year_rank DESC, id) AS latest_rank
        FROM scored_promotions
    ) x
    WHERE latest_rank = 1
),
enriched AS (
    SELECT
        *,
        AVG(promo_code_max_starts_on_year) OVER w_grp AS promo_code_max_starts_on_year_avg_5,
        AVG(cumulative_starts_on_year_cume_dist) OVER w_hist AS cumulative_starts_on_year_cume_dist_avg_2,
        SUM(starts_on_year_rank) OVER w_running AS promo_code_sum_starts_on_year_rank,
        ROUND(row_seq / SUM(row_seq) OVER (PARTITION BY promo_code), 4) AS row_seq_share
    FROM latest
    WINDOW w_grp AS (PARTITION BY promo_code ORDER BY row_seq, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW), w_hist AS (PARTITION BY promo_code ORDER BY starts_on, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), w_running AS (PARTITION BY promo_code)
),
latest_promotions AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY ends_on, id) AS latest_rank_2
        FROM enriched
    ) ranked
    WHERE latest_rank_2 = 1
),
stats AS (
    SELECT
        promo_code,
        COUNT(DISTINCT promo_code) AS distinct_promo_codes,
        COUNT(DISTINCT promo_code) AS distinct_promo_codes_2,
        SORT_ARRAY(COLLECT_SET(promo_code)) AS promo_code_list
    FROM latest_promotions
    GROUP BY promo_code
)
SELECT promo_code, distinct_promo_codes
FROM stats
ORDER BY distinct_promo_codes NULLS LAST, promo_code DESC;
