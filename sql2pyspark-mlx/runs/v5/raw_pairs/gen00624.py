from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("quantity_whole")
w2 = Window.partitionBy("product_id").orderBy("quantity")
revenue_by_product_id = df_store_sales.groupBy("product_id").agg(F.avg("revenue").alias("avg_store_sale_revenue"))

cleaned = (
    df_inventory
    .filter((F.col("last_restock") >= F.lit("2023-08-01").cast("date")) & F.col("store_id").isin([2, 6]))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        F.ceil("quantity").alias("quantity_whole"),
        F.last_day("last_restock").alias("last_restock_month_end"),
    )
)

deduped = (
    cleaned
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
)

joined = (
    deduped
    .alias("s")
    .join(df_products.alias("d"), F.col("s.product_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.product_name"), F.col("d.brand"), F.col("d.category"))
)

latest = (
    joined
    .withColumn("rn", F.rank().over(w2))
    .filter(F.col("rn") == 1)
)

result = (
    latest
    .join(revenue_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "product_id",
        "quantity",
        "last_restock_month_end",
        "dedup_rank",
        "product_name",
        "brand",
        F.col("avg_store_sale_revenue"),
    )
    .orderBy(F.col("product_id").desc_nulls_last())
)
