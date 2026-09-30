from pyspark.sql import functions as F

with_dims = (
    df_order_items
    .alias("a")
    .join(df_orders.alias("d"), F.col("a.order_id") == F.col("d.id"), "left")
    .select(
        "a.id",
        "a.order_id",
        "a.quantity",
        F.col("d.channel"),
        F.col("d.order_date"),
    )
)

result = (
    with_dims
    .groupBy(
        F.when(F.col("quantity") >= 8, "LARGE").otherwise("SMALL").alias("quantity_tier"),
    )
    .agg(
        F.count(F.when(F.col("channel") == "partner", 1)).alias("partner_rows"),
        F.bit_and("quantity").alias("quantity_bit_and"),
        F.count(F.when(F.col("channel") == "partner", 1)).alias("partner_rows_2"),
        F.count("*").alias("num_rows"),
    )
    .filter(F.col("quantity_bit_and") > 1)
)
