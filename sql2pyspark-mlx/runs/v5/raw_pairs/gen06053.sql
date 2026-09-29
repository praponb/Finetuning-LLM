select * from (select status, count(*) as row_count from subscriptions where start_date <= '2023-03-01' group by status) as sub where row_count > 7 order by row_count
