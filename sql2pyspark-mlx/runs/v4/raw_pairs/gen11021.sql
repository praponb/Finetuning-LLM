select id, method, substring(status, 1, 3) as status_prefix, trim(status) as clean_status from payments order by clean_status;
