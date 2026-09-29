select id, device, started_at from (select *, row_number() over (partition by device order by started_at desc, id) as rn from sessions where customer_id is not null) as t where rn <= 3;
