from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .filter(F.col("p.order_id").isNull())
    .groupBy(F.col("o.status"), F.year(F.col("o.order_date")).alias("order_year"))
    .agg(
        F.sum(F.when(F.col("p.status") == "FAILED", F.col("p.amount")).otherwise(0)).alias("failed_amount"),
    )
)
