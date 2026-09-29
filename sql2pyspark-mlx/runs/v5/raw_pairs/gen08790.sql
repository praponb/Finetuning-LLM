select hire_date from employees where level in ('L4', 'L2') union all select order_date as hire_date from orders;
