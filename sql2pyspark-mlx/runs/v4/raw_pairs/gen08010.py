from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("cost").desc(), "id")
w_recent = Window.partitionBy("brand", "category")
w3 = Window.partitionBy("supplier_id").orderBy(F.col("price").desc())
w_hist = Window.partitionBy("supplier_id").orderBy("price", "id").rowsBetween(-3, -1)
w5 = Window.partitionBy("country", "supplier_id").orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("supplier_id", "country").orderBy("stock_qty", "id").rowsBetween(-6, Window.currentRow)

enriched = (
    df_products
    .alias("e")
    .join(df_suppliers.alias("p"), F.col("e.supplier_id") == F.col("p.id"), "inner")
    .select("e.*", F.col("p.country"))
)

with_windows = (
    enriched
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("brand_max_price", F.max("price").over(w_recent))
)

not_flagged = (
    with_windows
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.order_id").isin([33, 36])), "left_anti")
)

metrics = (
    not_flagged
    .withColumn("price_dense_rank", F.dense_rank().over(w3))
    .withColumn("row_seq_avg_3_prior", F.avg("row_seq").over(w_hist))
    .withColumn("cumulative_cost", F.sum("cost").over(w5))
    .withColumn("stock_qty_avg_6", F.avg("stock_qty").over(w6))
)

enriched_products = (
    metrics
    .alias("src")
    .join(df_suppliers.alias("p"), F.col("src.supplier_id") == F.col("p.id"), "left")
    .select(
        "src.*",
        F.col("p.supplier_name"),
        F.col("p.country").alias("supplier_country"),
    )
)

result = (
    enriched_products
    .groupBy("supplier_id")
    .pivot("supplier_country", ["US", "TH", "DE"])
    .agg(F.sum("cumulative_cost").alias("total"))
)
