SELECT firmware, device_code
FROM device_readings
EXCEPT DISTINCT
SELECT firmware, device_code
FROM device_readings
WHERE device_code IN ('DV-03', 'DV-02')
