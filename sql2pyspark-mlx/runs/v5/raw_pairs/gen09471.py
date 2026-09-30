from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("location").orderBy("dept_name_length", "id")
w3 = Window.partitionBy("dept_name").orderBy("dept_name_length", "id").rowsBetween(-5, -1)

filtered = (
    df_departments
    .filter(F.col("location").isNotNull() & (F.col("dept_name") == "HR"))
    .select(
        "*",
        F.length("dept_name").alias("dept_name_length"),
        F.coalesce("dept_name", F.lit("OTHER")).alias("resolved_dept_name"),
    )
)

result = (
    filtered
    .select(
        "id",
        "dept_name",
        "dept_name_length",
        F.last("location", ignorenulls=True).over(w).alias("last_known_location"),
        F.ntile(5).over(w2).alias("dept_name_length_bucket"),
        F.avg("budget").over(w3).alias("budget_avg_5_prior"),
    )
)
