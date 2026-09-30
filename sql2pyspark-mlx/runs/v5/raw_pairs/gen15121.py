from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("activity_month").orderBy(F.col("corr_cost_stock_qty").desc())

with_latest = (
    df_products
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.product_name",
        "s.cost",
        "s.stock_qty",
        "s.launch_date",
        F.col("c.quantity"),
        F.col("c.last_restock"),
    )
)

result = (
    with_latest
    .groupBy(
        F.date_trunc("month", "launch_date").alias("activity_month"),
        F.when(F.col("cost") >= 620, "LARGE").otherwise("SMALL").alias("cost_tier"),
    )
    .agg(
        F.bit_and("stock_qty").alias("stock_qty_bit_and"),
        F.corr("cost", "stock_qty").alias("corr_cost_stock_qty"),
        F.corr("cost", "stock_qty").alias("corr_cost_stock_qty_2"),
        F.percentile_approx("stock_qty", 0.5).alias("stock_qty_p50"),
        F.max("cost").alias("max_cost"),
    )
    .withColumn("corr_cost_stock_qty_dense_rank", F.dense_rank().over(w2))
)
