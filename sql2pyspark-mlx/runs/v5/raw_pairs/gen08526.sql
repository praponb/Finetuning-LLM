select dept_name, id from departments intersect all select dept_name, id from departments where location in ('Berlin', 'New York')
