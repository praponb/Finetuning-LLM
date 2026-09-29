from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("amount", "id")
w2 = Window.partitionBy("channel", "store_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_orders
    .withColumn("prev_amount", F.lag("amount", 1).over(w))
    .withColumn("last_known_shipping_fee", F.last("shipping_fee", ignorenulls=True).over(w2))
)

result = (
    enriched
    .select(
        "id",
        "customer_id",
        "store_id",
        "status",
        "amount",
        "discount",
        "shipping_fee",
        "last_known_shipping_fee",
        F.when(F.col("status").rlike("^(RETURNED|PENDING)$"), "PRIORITY").otherwise("CORE").alias("status_zone"),
    )
)
