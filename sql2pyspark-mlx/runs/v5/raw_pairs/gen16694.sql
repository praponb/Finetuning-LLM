SELECT device_code, firmware
FROM device_readings
INTERSECT
SELECT device_code, firmware
FROM device_readings
WHERE device_code = 'DV-01'
