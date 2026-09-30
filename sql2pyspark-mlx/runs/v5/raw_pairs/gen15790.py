from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("discount", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_windows = (
    df_orders
    .filter(F.col("store_id").isNull())
    .select(
        "*",
        F.avg("shipping_fee").over(w).alias("shipping_fee_avg_3"),
        F.last("discount", ignorenulls=True).over(w2).alias("last_known_discount"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "discount",
        "shipping_fee",
        "shipping_fee_avg_3",
        F.when(F.col("status").rlike("^(COMPLETED|RETURNED)$"), "EXTENDED").otherwise("GROUP_A").alias("status_zone"),
    )
    .orderBy(F.col("id").desc_nulls_first())
    .limit(10)
)
