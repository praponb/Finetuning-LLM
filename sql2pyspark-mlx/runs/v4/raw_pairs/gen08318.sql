select
    quarter(opened_date) as opened_quarter,
    status,
    min(opened_date) as first_opened_date,
    max(opened_date) as last_opened_date,
    count(case when status = 'OPEN' then 1 end) as open_count
from accounts
where month(opened_date) in (1, 6, 11)
group by 1, 2
order by opened_quarter desc;
