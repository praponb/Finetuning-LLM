from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("product_id").orderBy(F.col("last_restock_period").desc(), "id")
w2 = Window.orderBy(F.col("last_restock_period").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w3 = Window.orderBy(F.col("last_restock_period").cast("bigint")).rangeBetween(-7776000, Window.currentRow)

filtered = (
    df_inventory
    .filter(F.col("last_restock") >= F.lit("2024-04-23").cast("date"))
    .select("*", F.date_trunc("WEEK", "last_restock").alias("last_restock_period"))
)

result = (
    filtered
    .select(
        "*",
        F.row_number().over(w_ordered).alias("seq_no"),
        F.sum("quantity").over(w2).alias("quantity_sum_30d"),
        F.avg("quantity").over(w3).alias("quantity_avg_90d"),
    )
)
