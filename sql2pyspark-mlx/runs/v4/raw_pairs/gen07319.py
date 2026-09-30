from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "order_id").orderBy(F.col("delivered_date").desc(), "id")
quantity_by_order_id = df_order_items.groupBy("order_id").agg(F.sum("quantity").alias("sum_order_item_quantity"))

ranked = (
    df_shipments
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
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
        "rank_in_group",
        F.when(F.col("status").rlike("^(RETURNED|IN_TRANSIT)$"), "GROUP_A").otherwise("CORE").alias("status_zone"),
        F.col("sum_order_item_quantity"),
    )
    .orderBy(F.col("order_id").asc_nulls_first(), "shipped_date")
)
