with top_sessions as (select *, dense_rank() over (partition by device order by started_at desc) as row_num from sessions) select id, device, started_at from top_sessions where row_num <= 5
