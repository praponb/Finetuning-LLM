from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "country").orderBy(F.col("rating").desc(), "id")
w_part = Window.orderBy("rating", "id").rowsBetween(-5, -1)
w3 = Window.partitionBy("city", "country")
w4 = Window.partitionBy("city").orderBy("row_num", "id")
w5 = Window.partitionBy("city")
w6 = Window.partitionBy("country", "city").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("city").orderBy("rating_rounded", "id")
w8 = Window.partitionBy("country").orderBy("rating_rounded", "id")
w9 = Window.orderBy("rating_avg_5_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.orderBy("city_avg_rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.orderBy(F.col("next_rating_rounded").desc(), "id")
w_ordered = Window.partitionBy("country", "city").orderBy(F.col("rating_share").desc_nulls_first())

latest = (
    df_suppliers
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

with_windows = (
    latest
    .select(
        "*",
        F.avg("rating").over(w_part).alias("rating_avg_5_prior"),
        F.avg("rating").over(w3).alias("city_avg_rating"),
        F.lead("rating", 1).over(w4).alias("next_rating"),
        F.round((F.col("rating") / F.sum("rating").over(w5)), 4).alias("rating_share"),
        F.last("city", ignorenulls=True).over(w6).alias("last_known_city"),
    )
)

filtered = (
    with_windows
    .filter(F.col("country").isin(["US", "JP"]))
    .withColumn("rating_rounded", F.round("rating", 2))
    .withColumn("rating_filled", F.coalesce("rating", F.lit(0)))
)

kept = (
    filtered
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.category").isin(["Home", "Grocery", "Fashion"])), "left_anti")
)

with_windows_suppliers = (
    kept
    .withColumn("next_rating_rounded", F.lead("rating_rounded", 1).over(w7))
    .withColumn("prev_rating_filled", F.lag("rating_filled", 2).over(w8))
    .withColumn("running_city_avg_rating", F.sum("city_avg_rating").over(w9))
    .withColumn("cumulative_rating_avg_5_prior", F.sum("rating_avg_5_prior").over(w10))
)

metrics = (
    with_windows_suppliers
    .select(
        "*",
        F.row_number().over(w11).alias("event_seq"),
        F.dense_rank().over(w_ordered).alias("rating_share_dense_rank"),
    )
)

wide = (
    metrics
    .groupBy("city")
    .pivot("country", ["US", "UK"])
    .agg(F.avg("event_seq").alias("total"), F.count(F.lit(1)).alias("n"))
)

result = wide.select("city", "US_total", "UK_total")
