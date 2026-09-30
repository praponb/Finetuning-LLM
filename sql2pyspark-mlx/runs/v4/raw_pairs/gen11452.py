from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "region").orderBy(F.col("opened_date").asc_nulls_first(), "id")

filtered = (
    df_stores
    .filter((F.col("opened_date") > F.lit("2023-04-25").cast("date")) & F.col("region").isNotNull())
    .select(
        "*",
        F.when(F.col("region") == "North", 1).otherwise(0).alias("is_north"),
        F.when(F.col("city").isin(["Bangkok", "Chiang Mai"]), "Group A").otherwise("Other").alias("city_group"),
        F.coalesce("region", F.lit("UNKNOWN")).alias("resolved_region"),
    )
)

result = (
    filtered
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)
