select id, country, started_at, device from (select *, dense_rank() over (partition by country order by started_at desc) as rn from sessions) as r where rn <= 3 order by device
