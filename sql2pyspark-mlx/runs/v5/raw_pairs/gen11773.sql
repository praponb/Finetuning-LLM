select id, country, started_at, lag(started_at, 1) over (partition by country order by started_at, id) prev_started_at from sessions where country in ('SG', 'JP');
