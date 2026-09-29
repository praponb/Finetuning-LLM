from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name", "location").orderBy("budget", "id")

filtered = (
    df_departments
    .filter(F.col("location").isNotNull())
    .withColumn("adjusted_budget", F.round((F.col("budget") * 1.15), 2))
    .withColumn("resolved_dept_name", F.coalesce("dept_name", F.lit("NONE")))
)

result = (
    filtered
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)
