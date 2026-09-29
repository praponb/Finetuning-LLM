from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("hire_date_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("level").orderBy(F.col("hire_date_period").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w3 = Window.partitionBy("manager_id").orderBy("hire_date_period", "id")

filtered = (
    df_employees
    .filter(F.col("hire_date") > F.lit("2023-10-02").cast("date"))
    .withColumn("email_domain", F.regexp_extract("email", "@(.+)$", 1))
    .withColumn("hire_date_period", F.date_trunc("month", "hire_date"))
)

result = (
    filtered
    .select(
        "*",
        F.first("job_title", ignorenulls=True).over(w).alias("first_known_job_title"),
        F.sum("salary").over(w2).alias("salary_sum_7d"),
        F.lead("salary", 2).over(w3).alias("next_salary"),
    )
)
