from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.order_date").between("2025-08-23", "2025-09-01"))
    .groupBy(
        F.when(F.year(F.col("o.order_date")) < 2024, "before_2024").when(F.year(F.col("o.order_date")) == 2024, "2024").otherwise("after_2024").alias("order_cohort"),
        F.col("o.customer_id"),
    )
    .agg(
        F.min(F.col("p.paid_at")).alias("first_paid_at"),
        F.min(F.col("p.amount")).alias("min_amount"),
    )
    .orderBy("customer_id")
)
