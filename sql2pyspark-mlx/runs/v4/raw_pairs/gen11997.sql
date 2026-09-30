SELECT
    ss.id,
    ss.revenue,
    ss.store_id,
    s.opened_date
FROM store_sales ss
JOIN stores s ON ss.store_id = s.id
ORDER BY revenue DESC, id
LIMIT 10;
