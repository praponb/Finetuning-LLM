select
    id,
    salary,
    upper(trim(full_name)) as full_name_normalized,
    length(full_name) as full_name_length
from employees;
