from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("store_id", "product_id").orderBy("sale_date", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("store_id")
quantity_by_store_id = df_inventory.groupBy("store_id").agg(F.sum("quantity").alias("sum_inventory_quantity"))

metrics = (
    df_store_sales
    .select(
        "*",
        F.avg("revenue").over(w_part).alias("last3_avg_revenue"),
        F.min("revenue").over(w_part).alias("last3_min_revenue"),
        F.max("revenue").over(w_part).alias("last3_max_revenue"),
        F.sum("revenue").over(w_part).alias("last3_sum_revenue"),
        F.stddev_samp("revenue").over(w2).alias("store_id_stddev_samp_revenue"),
    )
)

result = (
    metrics
    .join(quantity_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "store_id",
        "product_id",
        "last3_avg_revenue",
        "last3_min_revenue",
        "last3_max_revenue",
        "last3_sum_revenue",
        "store_id_stddev_samp_revenue",
        F.col("sum_inventory_quantity"),
    )
    .orderBy(F.col("product_id").desc())
)
