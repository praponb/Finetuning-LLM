select id, browser, started_at from (select id, browser, started_at, row_number() over (partition by browser order by started_at desc, id) as rnk from sessions) as r where rnk = 1;
