with ranked_sessions as (select id, browser, started_at, dense_rank() over (partition by browser order by started_at desc) as rn from sessions) select * from ranked_sessions where rn = 1
