from pyspark.sql import functions as F

by_dimension = (
    df_order_items
    .alias("s")
    .join(df_orders.alias("dim"), F.col("s.order_id") == F.col("dim.id"), "left")
    .cube(F.col("dim.status").alias("status"))
    .agg(
        F.count("*").alias("num_rows"),
        F.round(F.sum(F.col("s.quantity")), 2).alias("total_quantity"),
    )
)

result = (
    by_dimension
    .select("status", "total_quantity")
    .orderBy(F.col("status").desc_nulls_last(), "total_quantity")
)
