from pyspark.sql import functions as F

result = (
    df_shipments
    .alias("e")
    .join(df_orders.alias("p"), F.col("e.order_id") == F.col("p.id"), "inner")
    .cube(F.col("p.channel").alias("channel"), F.col("e.carrier").alias("carrier"))
    .agg(
        F.count("*").alias("num_rows"),
        F.max(F.col("e.cost")).alias("max_cost"),
        F.grouping_id().alias("level_id"),
    )
    .orderBy(F.col("channel").asc_nulls_last())
)
