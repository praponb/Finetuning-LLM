from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "store_id").orderBy("shipping_fee", "id")
w2 = Window.partitionBy("store_id")

valid = (
    df_orders
    .filter((~F.col("channel").eqNullSafe(F.lit("store"))) & F.col("channel").isNotNull())
    .select(
        "*",
        F.when(F.col("status") == "COMPLETED", 1).otherwise(0).alias("is_completed"),
    )
)

result = (
    valid
    .select(
        "id",
        "store_id",
        "status",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        F.lead("discount", 1).over(w).alias("next_discount"),
        F.max("amount").over(w2).alias("store_id_max_amount"),
    )
)
