SELECT country FROM sessions WHERE device = 'tablet' INTERSECT SELECT country FROM sessions WHERE started_at >= '2023-06-01' ORDER BY country DESC;
