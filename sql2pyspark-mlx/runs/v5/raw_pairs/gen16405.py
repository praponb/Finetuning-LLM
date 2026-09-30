from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy(F.col("budget").desc_nulls_last(), "id")

valid = (
    df_departments
    .filter(F.col("dept_name").isin(["Legal", "Sales"]))
    .select(
        "*",
        F.substring("dept_name", 1, 2).alias("dept_name_prefix"),
        F.coalesce("location", F.lit("NONE")).alias("location_clean"),
    )
)

result = (
    valid
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "budget")
)
