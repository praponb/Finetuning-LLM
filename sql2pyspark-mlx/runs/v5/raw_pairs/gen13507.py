from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("last_restock", "id").rowsBetween(-6, -1)
w2 = Window.partitionBy("product_id").orderBy("quantity")
revenue_by_store_id = df_store_sales.groupBy("store_id").agg(F.avg("revenue").alias("avg_store_sale_revenue"))

enriched = (
    df_inventory
    .select(
        "*",
        F.avg("quantity").over(w).alias("quantity_avg_6_prior"),
        F.dense_rank().over(w2).alias("quantity_dense_rank"),
    )
)

result = (
    enriched
    .join(revenue_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "store_id",
        "product_id",
        "last_restock",
        "quantity_avg_6_prior",
        "quantity_dense_rank",
        F.col("avg_store_sale_revenue"),
    )
)
