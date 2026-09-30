from pyspark.sql import functions as F

base = (
    df_accounts
    .filter((F.col("opened_date") > F.lit("2023-04-07").cast("date")) & F.col("status").isNotNull() & (F.col("status") != "OPEN"))
    .withColumn("balance_int", F.col("balance").cast("int"))
    .withColumn("status_clean", F.coalesce("status", F.lit("NONE")))
)

result = (
    base
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.credit_limit"), F.col("d.city"))
)
