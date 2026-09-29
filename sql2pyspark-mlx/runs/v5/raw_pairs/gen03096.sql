SELECT country, TRIM(device) AS clean_device, INITCAP(TRIM(device)) AS device_title FROM sessions;
