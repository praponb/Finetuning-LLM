select is_remote, cast(is_remote as int) as is_remote_flag, cast(is_remote as int) as is_remote_flag_2
from employees
where upper(full_name) like '%IM %'
order by is_remote_flag desc;
