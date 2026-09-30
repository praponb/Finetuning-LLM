select id, browser from (select *, row_number() over (partition by customer_id order by started_at desc, id) as dedup_rank from sessions) x where dedup_rank = 1;
