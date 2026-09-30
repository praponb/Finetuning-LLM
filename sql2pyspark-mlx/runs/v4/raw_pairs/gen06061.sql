SELECT id, budget FROM departments WHERE location = 'New York' OR location IN ('Tokyo', 'Chiang Mai', 'London') ORDER BY id DESC
