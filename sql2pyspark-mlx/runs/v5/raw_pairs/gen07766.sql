with ranked as (select *, dense_rank() over (partition by carrier order by cost desc) as row_num from shipments where order_id = 36) select id, carrier, cost from ranked where row_num = 1
