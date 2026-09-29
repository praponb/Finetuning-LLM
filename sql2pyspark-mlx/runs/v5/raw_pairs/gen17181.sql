select id, device, started_at, rank() over (partition by device order by started_at desc) as rnk from sessions where customer_id in (1, 20, 32, 36);
