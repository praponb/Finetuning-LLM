from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("city").orderBy(F.col("rating").desc_nulls_first())
w3 = Window.partitionBy("country").orderBy("rating", "id")
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.price").desc(), F.col("c.id").desc())
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.stock_qty").desc(), F.col("c.id").desc())

enriched = (
    df_suppliers
    .withColumn("running_max_rating", F.max("rating").over(w))
    .withColumn("running_avg_rating", F.avg("rating").over(w))
    .withColumn("running_min_rating", F.min("rating").over(w))
    .withColumn("rating_percent_rank", F.percent_rank().over(w2))
)

with_windows = (
    enriched
    .filter(F.col("city") == "Bangkok")
    .select(
        "id",
        "supplier_name",
        "city",
        "rating",
        "running_max_rating",
        F.lag("running_max_rating").over(w3).alias("prev_running_max_rating"),
        F.lag("running_min_rating").over(w3).alias("prev_running_min_rating"),
        F.row_number().over(w3).alias("event_seq"),
    )
)

base = (
    with_windows
    .filter(F.col("prev_running_max_rating") < 2.7)
    .select("*", F.floor("running_max_rating").alias("running_max_rating_whole"))
)

not_flagged = (
    base
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.price") <= 410), "left_anti")
)

eligible = (
    not_flagged
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.stock_qty") != 291), "left_semi")
)

with_latest = (
    eligible
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.cost"))
)

latest_match = (
    with_latest
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.supplier_name",
        "s.city",
        "s.prev_running_max_rating",
        "s.prev_running_min_rating",
        "s.running_max_rating_whole",
        "s.cost",
        F.col("c.cost").alias("product_cost"),
    )
)

result = (
    latest_match
    .select("id", "city", "running_max_rating_whole", "cost")
    .orderBy("cost")
)
