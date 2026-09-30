from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category", "supplier_id").orderBy(F.col("cost").desc_nulls_first(), "id")
w2 = Window.partitionBy("supplier_id", "city").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.sale_date").desc(), F.col("c.id").desc())

latest = (
    df_products
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "category", "cost", "stock_qty", "launch_date", "tags", "supplier_id")
)

joined = (
    latest
    .alias("src")
    .join(df_suppliers.alias("s"), F.col("src.supplier_id") == F.col("s.id"), "left")
    .select("src.*", F.col("s.city"), F.col("s.supplier_name"))
)

metrics = (
    joined
    .select(
        "id",
        "category",
        F.min("stock_qty").over(w2).alias("running_min_stock_qty"),
        F.count("stock_qty").over(w2).alias("running_count_stock_qty"),
        F.avg("stock_qty").over(w2).alias("running_avg_stock_qty"),
    )
)

with_last = (
    metrics
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.units"))
)

result = (
    with_last
    .groupBy(
        F.when(F.col("running_count_stock_qty") >= 13, "LARGE").otherwise("SMALL").alias("running_count_stock_qty_tier"),
    )
    .agg(
        F.min("running_min_stock_qty").alias("min_running_min_stock_qty"),
        F.count(F.when(F.col("category") == "Sports", 1)).alias("sports_rows"),
        F.round(F.avg("units"), 2).alias("avg_units"),
        F.sum(F.when(F.col("category") == "Grocery", F.col("running_count_stock_qty"))).alias("grocery_total_running_count_stock_qty"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("event_count"),
    )
    .filter(F.col("sports_rows") > 1)
)
