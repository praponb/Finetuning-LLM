select dept_name, budget, location, initcap(trim(dept_name)) as dept_name_title from departments where location in ('Chiang Mai', 'Bangkok') order by dept_name_title;
