from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("opened_date").asc_nulls_last(), "id")

valid = (
    df_stores
    .filter((F.col("opened_date") > F.lit("2023-08-12").cast("date")) & (~F.col("region").eqNullSafe(F.lit("North"))))
    .select(
        "id",
        "city",
        "region",
        "opened_date",
        F.upper(F.trim("store_name")).alias("store_name_normalized"),
        F.coalesce("region", F.lit("DIRECT")).alias("resolved_region"),
    )
)

result = (
    valid
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "opened_date", "store_name_normalized")
)
