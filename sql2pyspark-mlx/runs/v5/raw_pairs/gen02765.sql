WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        session_id,
        event_type,
        page,
        duration_sec,
        ROUND(duration_sec, 2) AS duration_sec_rounded,
        CONCAT_WS('/', event_type, page) AS event_type_page_key
    FROM web_events
    WHERE event_time > DATE '2023-04-18'
      AND page IS NOT NULL
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        session_id,
        event_type,
        page,
        INITCAP(TRIM(page)) AS page_title
    FROM filtered
    WHERE page IS NOT NULL
),
valid_web_events AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN event_type IN ('page_view', 'click') THEN 'Tier 1' ELSE 'Other' END AS event_type_group,
        COALESCE(page, 'DIRECT') AS resolved_page
    FROM valid
    WHERE event_type IN ('page_view', 'add_to_cart')
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        session_id,
        page,
        page_title,
        event_type_group,
        resolved_page,
        CASE WHEN page = '/cart' THEN 1 ELSE 0 END AS is_cart
    FROM valid_web_events
    WHERE page = '/cart'
),
remaining_keys AS (
    -- Set difference on keys
    SELECT session_id
    FROM base
    EXCEPT ALL
    SELECT session_id
    FROM web_events
    WHERE page <> '/cart'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    INNER JOIN remaining_keys k ON s.session_id = k.session_id
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        page_title,
        event_type_group,
        is_cart,
        COUNT(is_cart) OVER (PARTITION BY session_id) AS session_id_count_is_cart,
        PERCENT_RANK() OVER (PARTITION BY session_id ORDER BY is_cart DESC) AS is_cart_percent_rank
    FROM verified
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        page_title,
        is_cart,
        session_id_count_is_cart,
        is_cart_percent_rank,
        DENSE_RANK() OVER (ORDER BY is_cart DESC NULLS LAST) AS is_cart_dense_rank,
        RANK() OVER (ORDER BY is_cart_percent_rank) AS is_cart_percent_rank_rank
    FROM scored
)
-- Multi-level aggregation
SELECT
    CASE WHEN is_cart_dense_rank >= 8 THEN 'HIGH' ELSE 'LOW' END AS is_cart_dense_rank_tier,
    PERCENTILE_APPROX(session_id_count_is_cart, ARRAY(0.25, 0.5, 0.75)) AS session_id_count_is_cart_quartiles,
    APPROX_PERCENTILE(is_cart, 0.9) AS is_cart_p90,
    APPROX_PERCENTILE(is_cart_dense_rank, 0.5) AS is_cart_dense_rank_p50
FROM metrics
GROUP BY CASE WHEN is_cart_dense_rank >= 8 THEN 'HIGH' ELSE 'LOW' END
ORDER BY is_cart_p90;
