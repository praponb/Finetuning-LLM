SELECT
    id,
    device,
    CASE WHEN country IN ('SG', 'JP') THEN 'Group A' ELSE 'Other' END country_group,
    CASE WHEN country = 'TH' THEN 1 ELSE 0 END is_th
FROM sessions;
