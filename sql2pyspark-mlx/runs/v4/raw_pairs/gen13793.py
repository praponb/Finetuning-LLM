from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("rating_filled", "id")
w2 = Window.orderBy(F.col("rating_filled").desc(), "id")
w_running = Window.partitionBy("country").orderBy("rating_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_suppliers
    .filter(F.col("rating") >= 3.2)
    .select(
        "id",
        "country",
        F.coalesce("rating", F.lit(0)).alias("rating_filled"),
        F.coalesce("city", F.lit("DIRECT")).alias("city_filled"),
    )
)

latest = (
    valid
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

metrics = (
    latest
    .withColumn("rating_filled_bucket", F.ntile(3).over(w2))
    .withColumn("running_rating_filled", F.sum("rating_filled").over(w_running))
    .withColumn("running_rating_filled_2", F.sum("rating_filled").over(w_running))
)

remaining_keys = (
    metrics
    .select("country")
    .intersect(
        df_suppliers
        .filter(F.col("rating") > 4.1)
        .select("country")
    )
)

verified = (
    metrics
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(
        F.when(F.col("rating_filled") >= 84, "LARGE").otherwise("SMALL").alias("rating_filled_tier"),
        F.when(F.col("running_rating_filled") >= 343, "LARGE").otherwise("SMALL").alias("running_rating_filled_tier"),
        "country",
    )
    .agg(
        F.avg(F.when(F.col("country") == "DE", F.col("running_rating_filled"))).alias("de_avg_running_rating_filled"),
        F.min("rating_filled").alias("min_rating_filled"),
        F.max("running_rating_filled_2").alias("max_running_rating_filled_2"),
    )
    .orderBy(F.col("min_rating_filled").asc_nulls_first())
)
