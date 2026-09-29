SELECT id, shipped_date, CASE WHEN status = 'LOST' THEN 1 ELSE 0 END AS is_lost FROM shipments WHERE shipped_date BETWEEN '2024-01-09' AND '2024-10-01' ORDER BY is_lost DESC
