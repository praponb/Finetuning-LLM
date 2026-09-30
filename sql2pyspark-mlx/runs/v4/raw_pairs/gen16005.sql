with filtered as (
    select *, month(hire_date) hire_date_month
    from employees
    where is_remote = true
)
select
    case when salary >= 162000 then 'HIGH' else 'LOW' end as salary_tier,
    percentile_approx(salary, 0.9) as salary_p90,
    percentile_approx(salary, array(0.25, 0.5, 0.75)) as salary_quartiles,
    percentile_approx(salary, array(0.25, 0.5, 0.75)) as salary_quartiles_2,
    count(*) as cnt
from filtered
group by case when salary >= 162000 then 'HIGH' else 'LOW' end
having salary_p90 < 0;
