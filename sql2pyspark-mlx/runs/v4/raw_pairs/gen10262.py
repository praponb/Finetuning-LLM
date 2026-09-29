from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("rating").asc_nulls_first(), "id")

cleaned = (
    df_suppliers
    .filter(F.col("city").isNotNull() & (F.col("city") != "Singapore"))
    .select(
        "id",
        "country",
        "rating",
        F.coalesce("city", F.lit("Unknown")).alias("city_filled"),
        F.coalesce("city", F.lit("UNKNOWN")).alias("city_filled_2"),
    )
)

result = (
    cleaned
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)
