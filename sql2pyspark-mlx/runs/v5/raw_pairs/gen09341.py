from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy(F.col("budget").asc_nulls_last(), "id")

deduped = (
    df_departments
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    deduped
    .select(
        "id",
        "location",
        "dedup_rank",
        F.when(F.col("location").rlike("^(London|Berlin)$"), "OTHER_ZONE").otherwise("GROUP_B").alias("location_zone"),
    )
)
