SELECT id, device, started_at, DENSE_RANK() OVER (PARTITION BY device ORDER BY started_at DESC) AS started_at_dense_rank FROM sessions WHERE browser != 'Firefox';
