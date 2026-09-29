select *
from (
    select status, max(opened_date) as last_opened_date, count(case when status = 'OPEN' then 1 end) as open_count
    from accounts
    where customer_id = 26
    group by status
) as agg
where open_count > 4
order by status
limit 3;
