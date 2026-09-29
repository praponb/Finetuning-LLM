SELECT customer_id, started_at FROM sessions WHERE device IN ('tablet', 'mobile') OR device = 'mobile';
