select id, level from (select *, row_number() over (partition by job_title order by salary, id) as dedup_rank from employees) ranked where dedup_rank = 1;
