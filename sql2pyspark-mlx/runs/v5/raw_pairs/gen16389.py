from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("salary", "id").rowsBetween(-2, -1)

filtered = (
    df_employees
    .filter(F.col("manager_id").isNotNull())
    .select("*", F.regexp_replace("full_name", " ", "_").alias("full_name_slug"))
)

result = (
    filtered
    .select(
        "id",
        "full_name",
        "email",
        "manager_id",
        "salary",
        "hire_date",
        "level",
        "full_name_slug",
        F.last("level", ignorenulls=True).over(w).alias("last_known_level"),
        F.avg("salary").over(w2).alias("salary_avg_2_prior"),
    )
)
