from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("city").orderBy(F.col("rating").asc_nulls_first())
w2 = Window.partitionBy("country").orderBy("rating", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.partitionBy("country")
w4 = Window.partitionBy("country").orderBy(F.col("rating_share").desc(), "id")

cleaned = (
    df_suppliers
    .filter(F.col("city").isNotNull() & (F.col("country") == "DE"))
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        F.trim("city").alias("clean_city"),
        F.coalesce("city", F.lit("N/A")).alias("resolved_city"),
    )
)

metrics = (
    cleaned
    .withColumn("rating_dense_rank", F.dense_rank().over(w_running))
    .withColumn("rating_avg_2", F.avg("rating").over(w2))
    .withColumn("rating_share", F.round((F.col("rating") / F.sum("rating").over(w3)), 4))
)

latest = (
    metrics
    .withColumn("dedup_rank", F.row_number().over(w4))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    latest
    .groupBy("country")
    .agg(
        F.stddev_samp("rating").alias("rating_stddev"),
        F.max("rating_avg_2").alias("max_rating_avg_2"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("cnt") > 1)
)
