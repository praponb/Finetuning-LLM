with Ranked as (select *, dense_rank() over (partition by status order by delivered_date desc) as pos from shipments) select id, status, delivered_date, carrier from Ranked where pos <= 3
