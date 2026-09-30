from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type", "customer_id").orderBy("age", "id")

enriched = (
    df_accounts
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .select("s.*", F.col("c.credit_limit"), F.col("c.age"))
)

latest = (
    enriched
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

enriched_accounts = (
    latest
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id") == F.col("c.id"), "left")
    .select(
        "e.*",
        F.col("c.segment"),
        F.col("c.credit_limit").alias("customer_credit_limit"),
        F.col("c.email"),
    )
)

result = (
    enriched_accounts
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "credit_limit",
        "age",
        "segment",
        "email",
    )
    .orderBy(F.col("id").desc())
)
