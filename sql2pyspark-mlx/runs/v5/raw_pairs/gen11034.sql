SELECT started_at, WEEKOFYEAR(started_at) AS started_at_weekofyear FROM sessions WHERE HOUR(started_at) BETWEEN 8 AND 11
