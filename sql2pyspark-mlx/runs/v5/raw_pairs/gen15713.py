from pyspark.sql import functions as F

dim_totals = (
    df_payments
    .alias("e")
    .join(df_orders.alias("dim"), F.col("e.order_id") == F.col("dim.id"), "inner")
    .cube(
        F.col("dim.status").alias("order_status"),
        F.col("e.status").alias("payment_status"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.max(F.col("e.amount")).alias("max_amount"),
    )
)

result = (
    dim_totals
    .select(
        "order_status",
        "payment_status",
        "max_amount",
        F.when(F.col("order_status").rlike("^(CANCELLED|PENDING)$"), "EXTENDED").when(F.col("order_status").rlike("^(COMPLETED|SHIPPED)$"), "CORE").otherwise("OTHER_ZONE").alias("order_status_zone"),
    )
    .orderBy(F.col("order_status_zone").asc_nulls_first())
)
