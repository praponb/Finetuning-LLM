from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.alias("p"), F.col("o.id") == F.col("p.order_id"), "left")
    .filter(F.col("o.order_date") >= "2023-05-01")
    .groupBy(F.col("o.id"), F.col("o.channel"))
    .agg(
        F.count(F.col("p.id")).alias("payment_count"),
        F.countDistinct(F.col("p.amount")).alias("unique_statuss"),
        (F.max(F.col("p.amount")) - F.min(F.col("p.amount"))).alias("amount_range"),
    )
    .orderBy(F.col("amount_range").desc(), F.col("unique_statuss").desc())
)
