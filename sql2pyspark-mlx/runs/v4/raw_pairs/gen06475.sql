select firmware, device_code
from device_readings
except
select firmware, device_code
from device_readings
where temp_c between 22 and 34
