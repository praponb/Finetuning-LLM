select count(case when status = 'DELIVERED' then 1 end) as delivered_count from shipments order by delivered_count;
