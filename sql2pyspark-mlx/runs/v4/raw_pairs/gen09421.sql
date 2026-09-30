WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT id, EXPLODE(tier_limits) AS element
    FROM price_tiers
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY element DESC NULLS LAST) AS element_cume_dist,
        CUME_DIST() OVER (ORDER BY element DESC) AS element_cume_dist_2,
        PERCENT_RANK() OVER (ORDER BY element ASC NULLS FIRST) AS element_percent_rank
    FROM exploded
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ROUND(element, 2) AS element_rounded
    FROM enriched
    WHERE element_cume_dist < 0.22
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        element,
        element_cume_dist,
        element_cume_dist_2,
        element_percent_rank,
        element_rounded,
        RANK() OVER (ORDER BY element_cume_dist DESC NULLS LAST) AS element_cume_dist_rank,
        RANK() OVER (ORDER BY element_rounded DESC NULLS FIRST) AS element_rounded_rank,
        PERCENT_RANK() OVER (ORDER BY element_rounded DESC NULLS LAST) AS element_rounded_percent_rank
    FROM base
),
enriched_price_tiers AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (ORDER BY element_rounded_percent_rank DESC) AS element_rounded_percent_rank_rank,
        RANK() OVER (ORDER BY element_rounded_rank DESC) AS element_rounded_rank_rank,
        PERCENT_RANK() OVER (ORDER BY element_cume_dist_2 ASC NULLS LAST) AS element_cume_dist_2_percent_rank,
        RANK() OVER w_grp AS element_rounded_percent_rank_rank_2
    FROM scored
    WINDOW w_grp AS (ORDER BY element_rounded_percent_rank DESC NULLS FIRST)
),
scored_price_tiers AS (
    -- Ranking and gap features
    SELECT
        id,
        element,
        element_cume_dist,
        element_cume_dist_2,
        element_percent_rank,
        element_rounded,
        element_cume_dist_rank,
        element_rounded_rank,
        element_rounded_percent_rank,
        element_rounded_percent_rank_rank,
        element_rounded_rank_rank,
        element_cume_dist_2_percent_rank,
        element_rounded_percent_rank_rank_2,
        CUME_DIST() OVER (ORDER BY element_rounded_percent_rank ASC) AS element_rounded_percent_rank_cume_dist,
        PERCENT_RANK() OVER (ORDER BY element_rounded_rank DESC NULLS LAST) AS element_rounded_rank_percent_rank,
        CUME_DIST() OVER (ORDER BY element_cume_dist_rank DESC NULLS LAST) AS element_cume_dist_rank_cume_dist
    FROM enriched_price_tiers
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY element_rounded_rank_rank DESC NULLS LAST) AS element_rounded_rank_rank_cume_dist,
        PERCENT_RANK() OVER w_running AS element_rounded_percent_rank_2,
        CUME_DIST() OVER (ORDER BY element_rounded_percent_rank_rank_2 DESC) AS element_rounded_percent_rank_rank_2_cume_dist,
        PERCENT_RANK() OVER w_running AS element_rounded_percent_rank_3,
        CUME_DIST() OVER (ORDER BY element_rounded ASC) AS element_rounded_cume_dist
    FROM scored_price_tiers
    WINDOW w_running AS (ORDER BY element_rounded DESC)
),
report AS (
    SELECT
        id,
        element,
        element_cume_dist,
        element_cume_dist_2,
        element_percent_rank,
        element_rounded,
        element_rounded_rank,
        element_rounded_percent_rank,
        element_rounded_percent_rank_rank
    FROM metrics
    ORDER BY element_rounded_percent_rank_rank DESC NULLS LAST
)
SELECT
    element,
    element_percent_rank,
    element_rounded,
    element_rounded_percent_rank
FROM report
ORDER BY element_percent_rank DESC NULLS LAST, element_rounded ASC;
