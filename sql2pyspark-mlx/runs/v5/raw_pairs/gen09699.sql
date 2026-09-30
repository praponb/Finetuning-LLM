select id, customer_id, device, browser, started_at, country from (select *, rank() over (partition by browser order by started_at desc nulls first) as rn from sessions) x where rn = 1;
