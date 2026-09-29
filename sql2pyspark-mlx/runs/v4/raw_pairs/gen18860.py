from pyspark.sql import functions as F

enriched = (
    df_order_items
    .alias("e")
    .join(df_orders.alias("b"), F.col("e.order_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.status"), F.col("b.discount"))
)

result = (
    enriched
    .groupBy(
        F.when(F.col("quantity") >= 8, "LARGE").otherwise("SMALL").alias("quantity_tier"),
    )
    .agg(
        F.size(F.collect_set("status")).alias("num_status"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.sort_array(F.collect_set("status")).alias("status_list_2"),
    )
    .filter(F.col("num_status") > 1)
)
