SELECT id, device FROM sessions WHERE customer_id IS NOT NULL INTERSECT SELECT id, device FROM sessions WHERE device IN ('tablet', 'mobile')
