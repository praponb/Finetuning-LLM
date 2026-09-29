select location, case when budget >= 3212000 then 'high' when budget >= 1305000 then 'mid' else 'low' end as budget_bucket, count(case when dept_name = 'Legal' then 1 end) as legal_count
from departments
group by location, case when budget >= 3212000 then 'high' when budget >= 1305000 then 'mid' else 'low' end
order by legal_count desc
