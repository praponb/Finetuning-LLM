from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("quantity").desc(), "id")
revenue_by_store_id = df_store_sales.groupBy("store_id").agg(F.max("revenue").alias("max_store_sale_revenue"))

ranked = (
    df_inventory
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    ranked
    .join(revenue_by_store_id, on="store_id", how="left")
    .select("id", "store_id", "product_id", "quantity", F.col("max_store_sale_revenue"))
    .orderBy(F.col("max_store_sale_revenue").desc(), "store_id")
)
