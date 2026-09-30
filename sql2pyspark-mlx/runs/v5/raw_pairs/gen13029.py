from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("budget", "id")

filtered = (
    df_departments
    .filter(F.col("dept_name").isin(["Engineering", "Marketing", "Legal"]))
    .select(
        "*",
        F.coalesce("location", F.lit("Unknown")).alias("location_filled"),
        F.coalesce("location", F.lit("DIRECT")).alias("location_clean"),
    )
)

result = (
    filtered
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)
