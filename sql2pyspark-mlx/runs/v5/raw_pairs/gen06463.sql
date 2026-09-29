SELECT
    id,
    customer_id,
    started_at,
    device,
    browser,
    RANK() OVER (PARTITION BY customer_id ORDER BY started_at DESC) AS rnk,
    RANK() OVER (PARTITION BY device, browser ORDER BY started_at) AS started_at_rank
FROM sessions
WHERE country <> 'SG';
