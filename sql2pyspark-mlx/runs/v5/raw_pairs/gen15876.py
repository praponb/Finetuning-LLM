from pyspark.sql import functions as F

customer_summary = (
    df_accounts
    .groupBy("customer_id")
    .agg(
        F.max("opened_date").alias("last_opened_date"),
        F.countDistinct("balance").alias("unique_statuss"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("C"), F.col("c.id") == F.col("C.customer_id"), "left")
    .filter(F.col("C.unique_statuss") > 6)
    .select(
        F.col("c.age"),
        F.col("c.country"),
        F.col("C.last_opened_date"),
        F.col("C.unique_statuss"),
    )
    .orderBy("unique_statuss")
)
