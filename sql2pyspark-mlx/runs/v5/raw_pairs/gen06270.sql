WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        DATE_FORMAT(starts_on, 'yyyy-MM-dd') starts_on_label,
        CASE WHEN discount_pct >= 0.29 THEN 'High' WHEN discount_pct >= 0.1 THEN 'Medium' ELSE 'Low' END discount_pct_band,
        COALESCE(channel, 'UNKNOWN') channel_clean
    FROM promotions
    WHERE channel IS DISTINCT FROM 'mobile'
),
cleaned_promotions AS (
    -- Keep recent, valid rows
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        starts_on_label,
        discount_pct_band,
        DATE_ADD(ends_on, 54) ends_on_plus_54d,
        LENGTH(promo_code) promo_code_length
    FROM cleaned
    WHERE channel IS NOT DISTINCT FROM 'partner'
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        starts_on_label,
        discount_pct_band,
        ends_on_plus_54d,
        ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY ends_on_plus_54d, id) row_seq,
        FIRST_VALUE(channel, TRUE) OVER (PARTITION BY promo_code ORDER BY starts_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_channel
    FROM cleaned_promotions
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY ends_on_plus_54d DESC NULLS FIRST, id) AS dedup_rank
    FROM scored
    QUALIFY dedup_rank = 1
),
deduped_promotions AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        channel,
        promo_code,
        discount_pct,
        discount_pct_band,
        ends_on_plus_54d,
        dedup_rank
    FROM deduped
    QUALIFY RANK() OVER (PARTITION BY channel, promo_code ORDER BY starts_on DESC) = 1
)
SELECT
    id,
    channel,
    promo_code,
    discount_pct,
    discount_pct_band,
    dedup_rank,
    CASE WHEN promo_code RLIKE '^(VIP|BUNDLE)$' THEN 'EXTENDED' ELSE 'CORE' END AS promo_code_zone
FROM deduped_promotions
ORDER BY promo_code DESC NULLS LAST, dedup_rank DESC;
