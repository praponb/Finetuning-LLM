from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.upper(F.trim("email")) == "KANYA@MAIL.CO.TH")
    .groupBy("job_title")
    .agg(
        F.max("hire_date").alias("last_hire_date"),
        (F.max("salary") - F.min("salary")).alias("salary_range"),
        F.count("*").alias("num_employees"),
    )
    .filter(F.col("num_employees") >= 2)
)
