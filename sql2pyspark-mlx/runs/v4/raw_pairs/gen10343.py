from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("rating", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("city").orderBy("rating", "id")
w3 = Window.partitionBy("country", "city").orderBy(F.col("rating").desc_nulls_last())
w4 = Window.orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("city").orderBy(F.col("rating_avg_3").asc_nulls_first())
w6 = Window.partitionBy("city", "country").orderBy(F.col("rating").asc_nulls_first())

with_windows = (
    df_suppliers
    .select(
        "*",
        F.avg("rating").over(w).alias("rating_avg_3"),
        F.row_number().over(w2).alias("seq_no"),
    )
)

enriched = (
    with_windows
    .withColumn("rating_percent_rank", F.percent_rank().over(w3))
    .withColumn("running_rating_avg_3", F.sum("rating_avg_3").over(w4))
)

matched_keys = (
    enriched
    .select("country", "id")
    .intersectAll(
        df_suppliers
        .filter(F.col("city") == "Berlin")
        .select("country", "id")
    )
)

reconciled = (
    enriched
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.country") == F.col("k.country")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

valid = (
    reconciled
    .filter(F.col("city").isNotNull())
    .withColumn("least_rating_avg_3_running_rating_avg_3", F.least("rating_avg_3", "running_rating_avg_3"))
)

eligible = (
    valid
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.stock_qty") == 300), "left_semi")
)

result = (
    eligible
    .select(
        "*",
        F.cume_dist().over(w_running).alias("rating_avg_3_cume_dist"),
        F.rank().over(w6).alias("rating_rank"),
    )
)
