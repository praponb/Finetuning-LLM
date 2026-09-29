WITH filtered AS (
    SELECT
        *,
        LENGTH(region) region_length,
        LAST_DAY(opened_date) opened_date_month_end,
        COALESCE(region, 'UNKNOWN') region_clean
    FROM stores
    WHERE opened_date >= DATE '2023-09-14'
      AND region IS NOT NULL
),
remaining_keys AS (
    SELECT city, id
    FROM filtered
    INTERSECT
    SELECT city, id
    FROM stores
    WHERE opened_date > '2024-06-01'
),
reconciled AS (
    SELECT s.*
    FROM filtered s
    JOIN remaining_keys k ON s.city = k.city AND s.id = k.id
),
metrics AS (
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        region_length,
        opened_date_month_end,
        region_clean,
        PERCENT_RANK() OVER (PARTITION BY city ORDER BY region_length) region_length_percent_rank,
        STDDEV_SAMP(sqft) OVER w_ordered city_stddev_samp_sqft,
        PERCENT_RANK() OVER w_recent region_length_percent_rank_2
    FROM reconciled
    WINDOW w_ordered AS (PARTITION BY city), w_recent AS (ORDER BY region_length DESC NULLS FIRST)
),
with_windows AS (
    SELECT
        *,
        MAX(region_length) OVER (PARTITION BY region) region_max_region_length,
        SUM(region_length_percent_rank) OVER (PARTITION BY city) city_sum_region_length_percent_rank,
        PERCENT_RANK() OVER (ORDER BY sqft DESC) sqft_percent_rank,
        CUME_DIST() OVER w_ordered sqft_cume_dist
    FROM metrics
    WINDOW w_ordered AS (ORDER BY sqft DESC NULLS LAST)
)
SELECT *
FROM (
    SELECT
        region,
        city,
        region_length_percent_rank
    FROM with_windows
) src
PIVOT (
    AVG(region_length_percent_rank)
    FOR city IN ('Tokyo', 'Berlin')
)
