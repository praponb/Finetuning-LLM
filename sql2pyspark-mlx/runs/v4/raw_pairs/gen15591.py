from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id")
w2 = Window.partitionBy("store_id", "product_id").orderBy("days_since_last_restock", "id").rowsBetween(-3, Window.currentRow)

filtered = (
    df_inventory
    .filter((F.col("last_restock") > F.lit("2024-04-19").cast("date")) & (F.col("last_restock") >= "2024-12-01"))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        F.datediff(F.current_date(), "last_restock").alias("days_since_last_restock"),
        F.date_trunc("week", "last_restock").alias("last_restock_period"),
    )
)

result = (
    filtered
    .withColumn("product_id_max_quantity", F.max("quantity").over(w))
    .withColumn("quantity_avg_3", F.avg("quantity").over(w2))
)
