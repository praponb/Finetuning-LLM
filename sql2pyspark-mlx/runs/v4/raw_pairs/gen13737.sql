SELECT
    DATE_TRUNC('WEEK', ends_on) AS week_start,
    DATE_TRUNC('QUARTER', ends_on) AS quarter_start,
    DATE_TRUNC('YEAR', ends_on) AS year_start,
    PERCENTILE_APPROX(discount_pct, ARRAY(0.25, 0.5, 0.75)) AS discount_pct_quartiles,
    PERCENTILE_APPROX(discount_pct, 0.5) AS discount_pct_p50,
    SORT_ARRAY(COLLECT_SET(promo_code)) AS promo_code_list
FROM promotions
GROUP BY GROUPING SETS ((DATE_TRUNC('WEEK', ends_on), DATE_TRUNC('QUARTER', ends_on), DATE_TRUNC('YEAR', ends_on)), (DATE_TRUNC('WEEK', ends_on), DATE_TRUNC('QUARTER', ends_on)));
