SELECT
    name,
    email,
    LENGTH(name) AS name_length,
    CASE WHEN segment = 'SMB' THEN 1 ELSE 0 END AS is_smb
FROM customers
WHERE age >= 43;
