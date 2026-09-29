from pyspark.sql import functions as F

base = (
    df_accounts
    .filter(F.col("opened_date") > F.lit("2023-09-12").cast("date"))
    .withColumn("days_since_opened_date", F.datediff(F.current_date(), "opened_date"))
    .withColumn("balance_adjusted", F.round((F.col("balance") * 0.85), 2))
)

result = (
    base
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "left")
    .filter(F.length(F.col("p.name")) > 13)
    .select(
        "a.id",
        "a.customer_id",
        "a.balance",
        "a.status",
        "a.balance_adjusted",
        F.col("p.segment"),
        F.col("p.age"),
    )
)
