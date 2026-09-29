SELECT CASE WHEN location IN ('London', 'Berlin') THEN 'tier_1' ELSE 'other' END location_group, SUM(budget) total_budget FROM departments GROUP BY location_group
