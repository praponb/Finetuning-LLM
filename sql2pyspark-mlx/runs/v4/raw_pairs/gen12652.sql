SELECT
    id,
    rating,
    CASE WHEN rating >= 3.5 THEN 'High' WHEN rating >= 1.9 THEN 'Medium' ELSE 'Low' END rating_tier,
    COALESCE(supplier_name, 'unassigned') supplier_name_filled
FROM suppliers;
