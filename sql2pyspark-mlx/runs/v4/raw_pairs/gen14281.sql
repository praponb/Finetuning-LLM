select hire_date from employees where level in ('L4', 'L5') except select opened_date from stores where region in ('Central', 'West') order by hire_date;
