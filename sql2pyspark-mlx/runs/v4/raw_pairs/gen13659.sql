select location as label from departments where dept_name = 'Legal' union select city as label from stores order by label desc;
