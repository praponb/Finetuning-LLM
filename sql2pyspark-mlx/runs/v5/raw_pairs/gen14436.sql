SELECT id, country FROM sessions EXCEPT ALL SELECT id, country FROM sessions WHERE TO_DATE(started_at) >= '2024-02-27';
