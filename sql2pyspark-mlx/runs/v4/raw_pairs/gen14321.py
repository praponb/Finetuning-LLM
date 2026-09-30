from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("city").orderBy(F.col("rating").asc_nulls_last())
w3 = Window.partitionBy("city")
w4 = Window.partitionBy("city").orderBy(F.col("rating_rounded").desc(), "id")
w5 = Window.partitionBy("city").orderBy("rating_cume_dist", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("city_min_rating_rounded", "id")
w7 = Window.orderBy("city_min_rating_rounded", "id").rowsBetween(-3, Window.currentRow)
w8 = Window.partitionBy("city").orderBy("city_min_rating_rounded_avg_3", "id")

filtered = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "supplier_name",
        "city",
        "rating",
        F.round("rating", 1).alias("rating_rounded"),
    )
)

enriched = (
    filtered
    .select(
        "*",
        F.last("city", ignorenulls=True).over(w_part).alias("last_known_city"),
        F.cume_dist().over(w_running).alias("rating_cume_dist"),
        F.min("rating_rounded").over(w3).alias("city_min_rating_rounded"),
    )
)

enriched_suppliers = (
    enriched
    .withColumn("rating_rounded_bucket", F.ntile(10).over(w4))
    .withColumn("last_known_city_min_rating_rounded", F.last("city_min_rating_rounded", ignorenulls=True).over(w5))
    .withColumn("prev_city_min_rating_rounded", F.lag("city_min_rating_rounded", 2).over(w6))
    .withColumn("city_min_rating_rounded_avg_3", F.avg("city_min_rating_rounded").over(w7))
)

deduped = (
    enriched_suppliers
    .withColumn("rank_in_group", F.row_number().over(w8))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

result = (
    deduped
    .select(
        "id",
        "city",
        "rating",
        "rating_rounded",
        "last_known_city",
        "rating_cume_dist",
        "city_min_rating_rounded",
        "rating_rounded_bucket",
        "prev_city_min_rating_rounded",
    )
    .orderBy("rating_rounded")
)
