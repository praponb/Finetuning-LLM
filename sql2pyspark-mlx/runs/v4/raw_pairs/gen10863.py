from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy(F.col("budget").desc_nulls_first(), "id")

filtered = (
    df_departments
    .filter(~F.col("location").isin(["Bangkok", "Singapore"]))
    .select("*", F.upper("dept_name").alias("dept_name_upper"))
)

result = (
    filtered
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)
