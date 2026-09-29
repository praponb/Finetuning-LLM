select * from (select dept_name, location, budget from departments) src pivot (max(budget) as value_sum, count(1) as cnt for location in ('London' as london, 'Tokyo' as tokyo));
