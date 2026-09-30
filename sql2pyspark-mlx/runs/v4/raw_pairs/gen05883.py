from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("shipping_fee_minus_discount", "id")

cleaned = (
    df_orders
    .filter(F.col("store_id").isNotNull())
    .select(
        "id",
        "order_date",
        "status",
        "amount",
        "discount",
        "shipping_fee",
        (F.col("shipping_fee") - F.col("discount")).alias("shipping_fee_minus_discount"),
    )
)

result = (
    cleaned
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)
