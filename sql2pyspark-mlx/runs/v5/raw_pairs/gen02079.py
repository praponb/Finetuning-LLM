from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("units").desc(), "id")
w2 = Window.orderBy(F.col("units").desc_nulls_last())
w3 = Window.partitionBy("store_id")
w4 = Window.partitionBy("product_id").orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
quantity_by_store_id = df_inventory.groupBy("store_id").agg(F.max("quantity").alias("max_inventory_quantity"))

enriched = (
    df_store_sales
    .withColumn("units_bucket", F.ntile(4).over(w))
    .withColumn("units_dense_rank", F.dense_rank().over(w2))
    .withColumn("revenue_share", F.round((F.col("revenue") / F.sum("revenue").over(w3)), 4))
    .withColumn("cumulative_revenue", F.sum("revenue").over(w4))
)

result = (
    enriched
    .join(quantity_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        "units_bucket",
        "units_dense_rank",
        "revenue_share",
        F.col("max_inventory_quantity"),
    )
)
