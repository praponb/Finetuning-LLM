SELECT browser, device FROM sessions EXCEPT SELECT browser, device FROM sessions WHERE country IN ('US', 'TH', 'UK');
