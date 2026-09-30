from pyspark.sql import functions as F

dim_totals = (
    df_shipments
    .alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .rollup(
        F.col("o.status").alias("order_status"),
        F.col("s.carrier").alias("carrier"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.max(F.col("s.cost")).alias("max_cost"),
        F.grouping_id().alias("level_id"),
    )
)

result = (
    dim_totals
    .select(
        "order_status",
        "max_cost",
        "level_id",
        F.when(F.col("order_status").rlike("^(RETURNED|PENDING)$"), "GROUP_B").when(F.col("order_status").rlike("^(COMPLETED|CANCELLED)$"), "OTHER_ZONE").otherwise("CORE").alias("order_status_zone"),
    )
    .orderBy(F.col("max_cost").desc(), "level_id")
)
