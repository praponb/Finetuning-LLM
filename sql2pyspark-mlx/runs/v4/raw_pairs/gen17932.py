from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())

base = (
    df_products
    .filter((F.col("launch_date") > F.lit("2023-03-28").cast("date")) & F.col("supplier_id").isNotNull())
    .withColumn("price_level", F.when(F.col("price") >= 1070, "High").when(F.col("price") >= 230, "Medium").otherwise("Low"))
    .withColumn("brand_clean", F.coalesce("brand", F.lit("UNKNOWN")))
)

with_last = (
    base
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.quantity").alias("inventory_quantity"))
)

not_flagged = (
    with_last
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.revenue") < 2200), "left_anti")
)

result = (
    not_flagged
    .groupBy("category", F.date_trunc("month", "launch_date").alias("activity_month"))
    .agg(
        F.count("*").alias("num_rows"),
        F.round(F.avg("stock_qty"), 2).alias("avg_stock_qty"),
    )
    .orderBy(F.col("num_rows").desc())
)
