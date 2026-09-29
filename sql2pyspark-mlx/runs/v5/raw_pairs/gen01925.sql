select email from customers where segment = 'Consumer' union select email from employees where manager_id is null
