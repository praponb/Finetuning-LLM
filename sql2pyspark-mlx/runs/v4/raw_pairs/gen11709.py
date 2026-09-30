from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("category").orderBy(F.col("stock_qty").desc_nulls_last())
w2 = Window.partitionBy("brand", "supplier_id").orderBy(F.col("cost").desc())
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w4 = Window.orderBy(F.col("cost_dense_rank").desc(), "id")
w5 = Window.partitionBy("category").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("supplier_id").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_products
    .select(
        "*",
        F.percent_rank().over(w_grp).alias("stock_qty_percent_rank"),
        F.dense_rank().over(w2).alias("cost_dense_rank"),
    )
)

with_latest = (
    scored
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.last_restock").alias("inventory_last_restock"))
)

metrics = (
    with_latest
    .withColumn("row_seq", F.row_number().over(w4))
    .withColumn("cumulative_price", F.sum("price").over(w5))
    .withColumn("last_known_stock_qty", F.last("stock_qty", ignorenulls=True).over(w6))
)

result = (
    metrics
    .groupBy(
        F.when(F.col("stock_qty_percent_rank") >= 0.61, "PREMIUM").otherwise("STANDARD").alias("stock_qty_percent_rank_tier"),
    )
    .agg(
        F.percentile_approx("stock_qty_percent_rank", 0.95).alias("stock_qty_percent_rank_p95"),
        F.bit_and("stock_qty").alias("stock_qty_bit_and"),
        F.sort_array(F.collect_set("brand")).alias("brand_list"),
        F.round(F.sum("last_known_stock_qty") / F.nullif(F.count(F.when(F.col("brand") == "Acme", 1)), F.lit(0)), 2).alias("last_known_stock_qty_per_acme"),
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.sum(F.when(F.col("brand") == "Acme", F.col("row_seq")).otherwise(0)).alias("acme_row_seq"),
    )
)
