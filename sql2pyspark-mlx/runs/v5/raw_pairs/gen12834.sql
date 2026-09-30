select dept_name, case when dept_name in ('Sales', 'Engineering') then 'priority' else 'other' end as dept_name_group, count(*) as cnt from departments group by 1, 2;
