WITH lines AS (
    SELECT
        id,
        coupon_codes,
        attr_key,
        v
    FROM carts
    LATERAL VIEW EXPLODE(attributes) li AS attr_key, v
),
exploded AS (
    SELECT
        id,
        attr_key,
        tag
    FROM lines
    LATERAL VIEW EXPLODE(coupon_codes) li AS tag
),
cleaned AS (
    SELECT
        *,
        UPPER(attr_key) AS attr_key_upper,
        COALESCE(tag, 'OTHER') AS tag_filled
    FROM exploded
    WHERE attr_key IS NOT NULL
),
base AS (
    SELECT
        *,
        LENGTH(attr_key) AS attr_key_length,
        SUBSTRING(tag, 1, 3) AS tag_prefix,
        COALESCE(attr_key, 'OTHER') AS attr_key_clean
    FROM cleaned
    WHERE attr_key IS NOT NULL
),
with_windows AS (
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY attr_key_length DESC NULLS LAST) AS attr_key_length_cume_dist,
        COUNT(attr_key_length) OVER (PARTITION BY tag) AS tag_count_attr_key_length
    FROM base
),
metrics AS (
    SELECT
        id,
        attr_key,
        tag,
        attr_key_upper,
        tag_filled,
        attr_key_length,
        tag_prefix,
        attr_key_clean,
        attr_key_length_cume_dist,
        tag_count_attr_key_length,
        CUME_DIST() OVER (PARTITION BY attr_key ORDER BY tag_count_attr_key_length ASC) AS tag_count_attr_key_length_cume_dist,
        PERCENT_RANK() OVER (PARTITION BY attr_key ORDER BY attr_key_length_cume_dist ASC) AS attr_key_length_cume_dist_percent_rank,
        MIN(attr_key_length) OVER (PARTITION BY attr_key) AS attr_key_min_attr_key_length,
        DENSE_RANK() OVER (PARTITION BY tag ORDER BY attr_key_length ASC NULLS FIRST) AS attr_key_length_dense_rank
    FROM with_windows
),
grouped AS (
    SELECT
        CASE WHEN attr_key_length_cume_dist >= 0.84 THEN 'HIGH' ELSE 'LOW' END AS attr_key_length_cume_dist_tier,
        ROUND(AVG(attr_key_min_attr_key_length), 2) AS avg_attr_key_min_attr_key_length,
        COUNT(DISTINCT tag) AS distinct_tags,
        ROUND(SUM(tag_count_attr_key_length) / NULLIF(COUNT(CASE WHEN tag = 'BUNDLE' THEN 1 END), 0), 2) AS tag_count_attr_key_length_per_bundle,
        COUNT(DISTINCT attr_key) AS distinct_attr_keys
    FROM metrics
    GROUP BY attr_key_length_cume_dist_tier
)
SELECT
    attr_key_length_cume_dist_tier,
    avg_attr_key_min_attr_key_length,
    distinct_tags,
    tag_count_attr_key_length_per_bundle,
    distinct_attr_keys
FROM grouped
ORDER BY tag_count_attr_key_length_per_bundle ASC, attr_key_length_cume_dist_tier ASC
LIMIT 10
