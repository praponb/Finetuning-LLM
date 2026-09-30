from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("amount").desc(), "id")

result = (
    df_orders
    .filter(F.col("shipping_fee") >= 30)
    .select(
        "id",
        "store_id",
        "amount",
        "shipping_fee",
        F.row_number().over(w).alias("rn"),
    )
    .filter(F.col("rn") == 1)
    .select("id", "store_id", "amount", "shipping_fee")
)
