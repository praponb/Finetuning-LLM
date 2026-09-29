SELECT id, country, started_at FROM sessions WHERE device IN ('tablet', 'mobile') AND (started_at >= '2023-11-01' OR country IN ('US', 'JP', 'SG')) ORDER BY id DESC LIMIT 10
