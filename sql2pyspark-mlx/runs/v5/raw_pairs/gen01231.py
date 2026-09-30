from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name", "location").orderBy(F.col("budget_rounded").desc_nulls_first(), "id")

base = (
    df_departments
    .filter(F.col("dept_name").isNotNull())
    .withColumn("budget_rounded", F.round("budget", 0))
    .withColumn("location_clean", F.coalesce("location", F.lit("NONE")))
)

result = (
    base
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)
