select location from departments where dept_name in ('Finance', 'Engineering') union select location from departments where budget between 3532000 and 3729000 order by location
