from pyspark.sql import functions as F

src = (
    df_accounts
    .filter(~(F.col("customer_id").isNull() & F.col("balance").isNull() & F.col("customer_id").isNotNull()))
)

customer_id_summary = (
    src
    .groupBy("customer_id")
    .agg(
        F.avg("balance").alias("avg_balance"),
        F.count("*").alias("row_count"),
    )
)

result = (
    customer_id_summary
    .filter(F.col("avg_balance") < 53000)
    .select("customer_id", "row_count")
    .orderBy(F.col("row_count").desc())
)
