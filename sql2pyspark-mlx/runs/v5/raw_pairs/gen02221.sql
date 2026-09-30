SELECT c.id, c.segment FROM customers c WHERE c.city <> 'Chiang Mai' AND c.id IN (SELECT customer_id FROM orders WHERE channel <> 'mobile') ORDER BY segment ASC
