select
    case when dept_name in ('Engineering', 'Marketing') then 'tier_1' else 'other' end as dept_name_group,
    round((100 * sum(case when dept_name = 'Finance' then 1 else 0 end)) / count(*), 1) as pct_finance,
    round((100 * sum(case when dept_name = 'Sales' then 1 else 0 end)) / count(*), 1) as pct_sales,
    count(*) as total_count
from departments
where dept_name in ('Sales', 'Legal', 'HR') and budget between 2447000 and 3897000
group by dept_name_group
order by dept_name_group desc;
