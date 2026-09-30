select *, row_number() over (partition by device_code order by humidity nulls last, id) as row_num
from device_readings
qualify row_num = 1
