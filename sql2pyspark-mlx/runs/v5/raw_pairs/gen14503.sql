WITH valid_stores AS (
    SELECT *, DATE_FORMAT(opened_date, 'yyyy-MM-dd') opened_date_label
    FROM stores
    WHERE city = 'London' AND opened_date < '2024-05-01'
),
agg AS (
    SELECT city, CASE WHEN region IN ('Central', 'South') THEN 'tier_1' ELSE 'other' END region_group, SORT_ARRAY(COLLECT_SET(region)) region_list
    FROM valid_stores
    GROUP BY city, CASE WHEN region IN ('Central', 'South') THEN 'tier_1' ELSE 'other' END
)
SELECT city, region_group
FROM agg;
