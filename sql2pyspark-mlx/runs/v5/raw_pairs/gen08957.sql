WITH filtered AS (
    SELECT
        id,
        channel,
        ends_on,
        discount_pct,
        DATE_TRUNC('month', ends_on) AS ends_on_period,
        DATEDIFF(starts_on, ends_on) AS days_ends_on_to_starts_on
    FROM promotions
    WHERE ends_on >= TO_DATE('2023-07-10')
)
SELECT
    *,
    NTILE(10) OVER w_part AS days_ends_on_to_starts_on_bucket,
    NTILE(10) OVER w_part AS days_ends_on_to_starts_on_bucket_2,
    RANK() OVER (PARTITION BY channel ORDER BY days_ends_on_to_starts_on NULLS LAST) AS days_ends_on_to_starts_on_rank
FROM filtered
WHERE TO_DATE(ends_on_period) >= '2025-05-04'
WINDOW w_part AS (PARTITION BY channel ORDER BY days_ends_on_to_starts_on, id)
