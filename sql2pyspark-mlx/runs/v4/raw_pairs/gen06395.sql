SELECT device_code, id FROM device_readings INTERSECT SELECT device_code, id FROM device_readings WHERE humidity IS NOT NULL;
