from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("hire_date", "id")
w2 = Window.partitionBy("dept_id").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_employees
    .select(
        "*",
        F.lag("salary", 1).over(w).alias("prev_salary"),
        F.first("level", ignorenulls=True).over(w2).alias("first_known_level"),
        F.sum("salary").over(w3).alias("cumulative_salary"),
    )
)

result = (
    scored
    .groupBy(F.date_trunc("WEEK", "hire_date").alias("week_start"))
    .agg(
        F.sum(F.when(F.col("job_title") == "Intern", F.col("cumulative_salary")).otherwise(0)).alias("intern_cumulative_salary"),
        F.sum(F.when(F.col("level") == "L2", F.col("prev_salary")).otherwise(0)).alias("l2_prev_salary"),
        F.count("*").alias("num_rows"),
    )
)
