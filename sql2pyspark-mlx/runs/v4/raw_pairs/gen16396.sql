select * from (select *, row_number() over (partition by job_title order by salary desc, id) as rnk from employees where job_title in ('Director', 'Engineer')) as t where rnk <= 3 order by salary
