from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "status").orderBy(F.col("discount").asc_nulls_last())
w2 = Window.partitionBy("channel").orderBy(F.col("order_date").desc(), "id")
w3 = Window.partitionBy("customer_id", "store_id").orderBy(F.col("amount").desc_nulls_last())
w4 = Window.partitionBy("customer_id", "store_id").orderBy("discount", "id").rowsBetween(-3, Window.currentRow)
quantity_by_store_id = df_inventory.groupBy("store_id").agg(F.min("quantity").alias("min_inventory_quantity"))

with_windows = (
    df_orders
    .select(
        "*",
        F.rank().over(w).alias("discount_rank"),
        F.row_number().over(w2).alias("event_seq"),
        F.rank().over(w3).alias("amount_rank"),
        F.avg("amount").over(w4).alias("amount_avg_3"),
    )
)

result = (
    with_windows
    .join(quantity_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "store_id",
        "status",
        "amount",
        "discount",
        "shipping_fee",
        "event_seq",
        "amount_rank",
        "amount_avg_3",
        F.when(F.col("status").rlike("^(RETURNED|SHIPPED)$"), "EXTENDED").when(F.col("status").rlike("^(COMPLETED|PENDING)$"), "PRIORITY").otherwise("CORE").alias("status_zone"),
        F.col("min_inventory_quantity"),
    )
)
