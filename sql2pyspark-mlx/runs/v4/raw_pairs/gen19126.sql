select level, count(*) row_count from employees group by level having max(salary) >= 128000;
