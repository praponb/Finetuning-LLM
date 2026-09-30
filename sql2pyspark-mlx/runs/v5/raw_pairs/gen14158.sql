select * from (select *, row_number() over (partition by device_code order by pressure asc, id asc) as row_num from device_readings) t where row_num = 1;
