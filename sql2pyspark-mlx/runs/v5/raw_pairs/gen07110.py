from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("city").orderBy("rating", "id")
w3 = Window.partitionBy("country").orderBy("rating", "id")
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.price").desc(), F.col("c.id").desc())
w5 = Window.partitionBy("country")
w6 = Window.partitionBy("country").orderBy(F.col("product_stock_qty").desc_nulls_last())
w7 = Window.partitionBy("country").orderBy(F.col("running_rating").asc_nulls_last())
w8 = Window.orderBy(F.col("product_stock_qty").desc(), "id")
w9 = Window.partitionBy("city").orderBy(F.col("product_stock_qty_rank").desc_nulls_first(), "id")

valid = (
    df_suppliers
    .filter(F.col("city").isNotNull() & (F.col("rating") < 2.6))
    .select(
        "id",
        "country",
        "city",
        "rating",
        F.concat("country", F.lit(" - "), "city").alias("country_city"),
        F.coalesce("city", F.lit("DIRECT")).alias("resolved_city"),
    )
)

eligible = (
    valid
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.category") == "Fashion"), "left_anti")
)

enriched = (
    eligible
    .filter(F.col("rating") >= 1.9)
    .withColumn("running_rating", F.sum("rating").over(w))
    .withColumn("event_seq", F.row_number().over(w2))
    .withColumn("prev_rating", F.lag("rating", 2).over(w3))
)

with_last = (
    enriched
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.country",
        "s.city",
        "s.country_city",
        "s.resolved_city",
        "s.running_rating",
        "s.prev_rating",
        F.col("c.stock_qty").alias("product_stock_qty"),
    )
)

enriched_suppliers = (
    with_last
    .withColumn("country_count_running_rating", F.count("running_rating").over(w5))
    .withColumn("product_stock_qty_rank", F.rank().over(w6))
    .withColumn("running_rating_cume_dist", F.cume_dist().over(w7))
    .withColumn("row_seq", F.row_number().over(w8))
)

ranked = (
    enriched_suppliers
    .withColumn("rank_in_group", F.row_number().over(w9))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    ranked
    .groupBy("city")
    .agg(
        F.count(F.when(F.col("country") == "JP", 1)).alias("jp_count"),
        F.size(F.collect_set("city")).alias("num_cities"),
        F.count(F.when(F.col("country") == "UK", 1)).alias("uk_count"),
    )
    .filter(F.col("num_cities") >= 1)
)
