select * from (select id, browser, started_at, rank() over (partition by browser order by started_at desc) as rnk from sessions where device <> 'desktop') as t where rnk <= 3 order by started_at
