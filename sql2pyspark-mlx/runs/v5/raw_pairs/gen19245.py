from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("unit_price").desc_nulls_first(), "id")
w2 = Window.partitionBy("order_id").orderBy("quantity", "id").rowsBetween(-5, -1)
w3 = Window.partitionBy("order_id").orderBy("quantity", "id")
w4 = Window.partitionBy("order_id").orderBy("quantity")
w5 = Window.partitionBy("order_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("quantity", "id")

cleaned = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select("*", F.col("quantity").cast("string").alias("quantity_str"))
)

latest = (
    cleaned
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "order_id", "quantity")
)

filtered = (
    latest
    .filter(F.col("quantity") > 6)
    .withColumn("quantity_band", F.when(F.col("quantity") >= 7, "High").when(F.col("quantity") >= 5, "Medium").otherwise("Low"))
)

filtered_order_items = (
    filtered
    .filter(F.col("order_id").isin([3, 20, 24, 27]))
    .select("*", F.col("quantity").cast("string").alias("quantity_num"))
)

scored = (
    filtered_order_items
    .filter(F.col("order_id").isin([13, 20, 37, 40]))
    .select(
        "id",
        "order_id",
        "quantity",
        "quantity_band",
        "quantity_num",
        F.avg("quantity").over(w2).alias("quantity_avg_5_prior"),
        F.ntile(5).over(w3).alias("quantity_bucket"),
        F.percent_rank().over(w4).alias("quantity_percent_rank"),
        F.sum("quantity").over(w5).alias("running_quantity"),
        F.ntile(10).over(w6).alias("quantity_bucket_2"),
    )
)

result = (
    scored
    .alias("e")
    .join(df_orders.alias("o"), F.col("e.order_id") == F.col("o.id"), "inner")
    .rollup(F.col("o.channel").alias("channel"))
    .agg(
        F.count("*").alias("rows_total"),
        F.avg(F.col("e.quantity_avg_5_prior")).alias("avg_quantity_avg_5_prior"),
        F.grouping_id().alias("level_id"),
    )
    .orderBy(F.col("rows_total").asc_nulls_first())
)
