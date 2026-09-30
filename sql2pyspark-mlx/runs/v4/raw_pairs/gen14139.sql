SELECT browser FROM sessions EXCEPT SELECT browser FROM sessions WHERE country IS NULL;
