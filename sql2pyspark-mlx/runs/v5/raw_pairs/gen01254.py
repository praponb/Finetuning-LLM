from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("cost").asc_nulls_first(), "id")
quantity_by_order_id = df_order_items.groupBy("order_id").agg(F.max("quantity").alias("max_order_item_quantity"))

ranked = (
    df_shipments
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

result = (
    ranked
    .join(quantity_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        "rn",
        F.when(F.col("status").rlike("^(RETURNED|DELIVERED)$"), "CORE").when(F.col("status").rlike("^(IN_TRANSIT|LOST)$"), "GROUP_B").otherwise("GROUP_A").alias("status_zone"),
        F.col("max_order_item_quantity"),
    )
    .orderBy("id")
)
