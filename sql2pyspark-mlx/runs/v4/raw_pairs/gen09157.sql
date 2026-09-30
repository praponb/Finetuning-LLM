select id, budget, dept_name, location from departments where budget between 2407000 and 2450000 and (location in ('Tokyo', 'New York') or budget <= 1926000)
