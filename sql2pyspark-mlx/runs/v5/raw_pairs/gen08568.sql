select email from customers where segment not in ('Corporate', 'SMB') except select email from employees order by email desc;
