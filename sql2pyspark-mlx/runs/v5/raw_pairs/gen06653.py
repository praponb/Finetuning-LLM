from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())

with_last = (
    df_products
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.brand",
        "s.price",
        F.col("c.unit_price"),
        F.col("c.quantity").alias("order_item_quantity"),
    )
)

result = (
    with_last
    .groupBy("brand")
    .agg(
        F.sort_array(F.collect_set("brand")).alias("brand_list"),
        F.count(F.when(F.col("brand") == "Umbrella", 1)).alias("umbrella_rows"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .filter(F.col("distinct_ids") < 1000)
)
