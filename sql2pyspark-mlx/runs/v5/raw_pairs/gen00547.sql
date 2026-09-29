SELECT country FROM sessions WHERE HOUR(started_at) BETWEEN 9 AND 16 UNION ALL SELECT country FROM sessions WHERE country IN ('DE', 'JP', 'UK') ORDER BY country DESC;
