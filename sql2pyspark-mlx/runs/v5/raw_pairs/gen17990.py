from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("brand")
w3 = Window.partitionBy("category").orderBy("price", "id").rowsBetween(-5, Window.currentRow)
w4 = Window.orderBy(F.col("cost").desc(), "id")
w5 = Window.partitionBy("supplier_id").orderBy(F.col("launch_date_plus_months").desc(), "id")
w6 = Window.partitionBy("supplier_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_products
    .filter((F.col("launch_date") >= F.lit("2023-03-17").cast("date")) & F.col("category").isin(["Electronics", "Home"]))
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "launch_date",
        "tags",
        "supplier_id",
        F.add_months("launch_date", 12).alias("launch_date_plus_months"),
        F.when(F.col("category") == "Home", 1).otherwise(0).alias("is_home"),
    )
)

with_latest = (
    cleaned
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.unit_price"))
)

metrics = (
    with_latest
    .withColumn("brand_avg_unit_price", F.avg("unit_price").over(w2))
    .withColumn("price_avg_5", F.avg("price").over(w3))
    .withColumn("cost_bucket", F.ntile(10).over(w4))
    .withColumn("event_seq", F.row_number().over(w5))
    .withColumn("running_unit_price", F.sum("unit_price").over(w6))
)

with_latest_products = (
    metrics
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.quantity"))
)

result = (
    with_latest_products
    .groupBy("supplier_id")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.max(F.when(F.col("brand") == "Umbrella", F.col("is_home"))).alias("umbrella_max_is_home"),
        F.count_if(F.col("brand") == "Globex").alias("globex_events"),
        F.max("quantity").alias("max_quantity"),
        F.round(F.sum("is_home") / F.nullif(F.count(F.when(F.col("brand") == "Umbrella", 1)), F.lit(0)), 2).alias("is_home_per_umbrella"),
    )
)
