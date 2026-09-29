WITH flattened AS (
    SELECT
        id,
        created_at,
        coupon_codes,
        pos,
        li_item.price
    FROM carts
    LATERAL VIEW OUTER POSEXPLODE(items) e AS pos, li_item
),
lines AS (
    SELECT
        id,
        created_at,
        pos,
        value
    FROM flattened
    LATERAL VIEW EXPLODE(coupon_codes) t AS value
),
with_windows AS (
    SELECT
        *,
        AVG(pos) OVER (PARTITION BY value ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS pos_avg_30m,
        COUNT(pos) OVER (PARTITION BY value) AS value_count_pos,
        MAX(pos) OVER (PARTITION BY value ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS pos_max_15m,
        CUME_DIST() OVER (PARTITION BY value ORDER BY pos) AS pos_cume_dist,
        MAX(pos) OVER (ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS pos_max_30m
    FROM lines
),
metrics AS (
    SELECT
        *,
        SUM(pos_max_30m) OVER (ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS pos_max_30m_sum_1h,
        COUNT(pos) OVER (PARTITION BY value) AS value_count_pos_2,
        DENSE_RANK() OVER (PARTITION BY value ORDER BY pos_avg_30m NULLS FIRST) AS pos_avg_30m_dense_rank,
        CUME_DIST() OVER (PARTITION BY value ORDER BY pos_max_15m NULLS FIRST) AS pos_max_15m_cume_dist,
        COUNT(pos_max_30m) OVER (PARTITION BY value ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS pos_max_30m_count_2h
    FROM with_windows
)
SELECT
    id,
    created_at,
    pos,
    value,
    value_count_pos,
    pos_cume_dist,
    pos_max_30m,
    pos_max_30m_sum_1h,
    pos_max_15m_cume_dist,
    CASE WHEN value RLIKE '^(SPRING10|WELCOME5)$' THEN 'EXTENDED' WHEN value RLIKE '^(VIP|BUNDLE)$' THEN 'OTHER_ZONE' ELSE 'GROUP_A' END AS value_zone
FROM metrics
ORDER BY pos_max_15m_cume_dist DESC NULLS FIRST
