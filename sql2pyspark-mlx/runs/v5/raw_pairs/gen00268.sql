select
    case when country in ('SG', 'UK') then 'core' else 'other' end as country_group,
    round((100 * sum(case when segment = 'SMB' then 1 else 0 end)) / count(*), 1) as pct_smb,
    round((100 * sum(case when city = 'Berlin' then 1 else 0 end)) / count(*), 1) as pct_berlin,
    count(distinct age) as unique_segments
from customers
where lower(trim(email)) = 'jack@startup.io'
group by case when country in ('SG', 'UK') then 'core' else 'other' end;
