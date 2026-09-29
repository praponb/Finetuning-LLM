from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name")

result = (
    df_departments
    .filter(F.col("location").isNull())
    .select(
        "id",
        "dept_name",
        "budget",
        F.avg("budget").over(w).alias("dept_name_avg_budget"),
    )
    .orderBy(F.col("id").desc())
)
