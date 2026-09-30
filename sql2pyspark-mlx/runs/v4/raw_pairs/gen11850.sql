select id, location, lower(dept_name) as dept_name_lower from departments where location is not null order by location;
