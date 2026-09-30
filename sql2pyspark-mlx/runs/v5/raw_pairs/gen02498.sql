select *, length(dept_name) as dept_name_length, case when dept_name in ('HR', 'Marketing') then 'Core' else 'Other' end as dept_name_group from departments order by 1 asc limit 20;
