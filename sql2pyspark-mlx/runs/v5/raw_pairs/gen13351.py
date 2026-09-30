from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "brand").orderBy(F.col("units").desc(), "id")
w2 = Window.partitionBy("brand", "product_id")
quantity_by_product_id = df_order_items.groupBy("product_id").agg(F.avg("quantity").alias("avg_order_item_quantity"))

joined = (
    df_store_sales
    .alias("a")
    .join(df_products.alias("d"), F.col("a.product_id") == F.col("d.id"), "left")
    .select("a.id", "a.product_id", "a.sale_date", "a.units", F.col("d.launch_date"))
)

joined_store_sales = (
    joined
    .alias("a")
    .join(df_products.alias("d"), F.col("a.product_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.brand"))
)

metrics = (
    joined_store_sales
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("units_share", F.round((F.col("units") / F.sum("units").over(w2)), 4))
)

filtered = (
    metrics
    .filter((F.col("brand").eqNullSafe(F.lit("Stark"))) & F.col("row_seq").isNotNull())
    .select("*", (F.col("row_seq") + F.col("units")).alias("row_seq_plus_units"))
)

result = (
    filtered
    .join(quantity_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "product_id",
        "sale_date",
        "units",
        "launch_date",
        "units_share",
        "row_seq_plus_units",
        F.col("avg_order_item_quantity"),
    )
)
