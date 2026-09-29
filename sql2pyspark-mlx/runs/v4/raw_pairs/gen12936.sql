WITH exploded AS (
    SELECT
        id,
        customer_id,
        tag
    FROM carts
    LATERAL VIEW OUTER EXPLODE(coupon_codes) e AS tag
),
base AS (
    SELECT *, UPPER(tag) AS tag_upper
    FROM exploded
    WHERE tag = 'WELCOME5'
),
joined AS (
    SELECT s.*, c.segment
    FROM base s
    INNER JOIN customers c
        ON s.customer_id = c.id
),
valid AS (
    SELECT
        id,
        customer_id,
        tag,
        segment,
        COALESCE(segment, 'N/A') AS segment_filled,
        COALESCE(segment, 'unassigned') AS segment_filled_2,
        COALESCE(tag, 'DIRECT') AS resolved_tag
    FROM joined
    WHERE customer_id = 4
)
SELECT
    segment,
    COUNT_IF(tag = 'BUNDLE') AS bundle_events,
    COUNT_IF(segment = 'Corporate') AS corporate_events,
    COUNT(*) AS cnt,
    SIZE(COLLECT_SET(segment)) AS num_segments,
    COUNT_IF(tag = 'WELCOME5') AS welcome5_events,
    SORT_ARRAY(COLLECT_SET(tag)) AS tag_list
FROM valid
GROUP BY 1
