from pyspark.sql import functions as F

filtered_employees = (
    df_employees
    .filter((F.col("dept_id") == 38) & F.col("level").isin(["L2", "L3", "L5"]) & (F.col("manager_id") == 11))
    .withColumn("email_length", F.length("email"))
)

hire_year_summary = (
    filtered_employees
    .groupBy(F.year("hire_date").alias("hire_year"))
    .agg(F.count("*").alias("num_employees"))
)

result = (
    hire_year_summary
    .filter(F.col("num_employees") > 4)
    .select("hire_year", "num_employees")
)
