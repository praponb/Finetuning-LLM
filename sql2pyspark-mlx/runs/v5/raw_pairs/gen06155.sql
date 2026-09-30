select full_name, date_format(hire_date, 'MMM yyyy') as hire_date_label from employees where dept_id = 19 and salary < 111000 order by 2, full_name desc;
