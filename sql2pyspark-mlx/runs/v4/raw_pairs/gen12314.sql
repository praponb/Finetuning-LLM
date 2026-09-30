select status, sum(case when status = 'RETURNED' then 1 else 0 end) as returned_count from orders group by status order by status, returned_count;
