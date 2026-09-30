from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.price").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("country").orderBy("rating", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.orderBy("product_stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("city", "country").orderBy(F.col("product_stock_qty").desc_nulls_first())
w5 = Window.partitionBy("country").orderBy(F.col("rating").desc_nulls_first(), "id")
w6 = Window.partitionBy("country")
w7 = Window.partitionBy("country", "city")
w8 = Window.partitionBy("city").orderBy(F.col("first_known_rating").desc(), "id")

cleaned = (
    df_suppliers
    .filter(F.col("country") != "UK")
    .select("*", F.upper("country").alias("country_upper"))
)

with_latest = (
    cleaned
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.stock_qty").alias("product_stock_qty"))
)

scored = (
    with_latest
    .withColumn("rating_avg_2", F.avg("rating").over(w2))
    .withColumn("first_known_rating", F.first("rating", ignorenulls=True).over(w3))
    .withColumn("product_stock_qty_rank", F.rank().over(w4))
)

ranked = (
    scored
    .withColumn("dedup_rank", F.row_number().over(w5))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

metrics = (
    ranked
    .select(
        "*",
        F.round((F.col("rating_avg_2") / F.sum("rating_avg_2").over(w6)), 4).alias("rating_avg_2_share"),
        F.round((F.col("rating") / F.sum("rating").over(w7)), 4).alias("rating_share"),
        F.row_number().over(w8).alias("seq_no"),
    )
)

result = (
    metrics
    .cube(
        F.when(F.col("product_stock_qty_rank") >= 7, "LARGE").otherwise("SMALL").alias("product_stock_qty_rank_tier"),
        "city",
    )
    .agg(
        F.size(F.collect_set("city")).alias("num_cities"),
        F.sum(F.when(F.col("country") == "UK", F.col("first_known_rating")).otherwise(0)).alias("uk_first_known_rating"),
        F.count(F.when(F.col("country") == "SG", 1)).alias("sg_count"),
        F.count("*").alias("row_count"),
    )
    .orderBy(F.col("sg_count").desc())
)
