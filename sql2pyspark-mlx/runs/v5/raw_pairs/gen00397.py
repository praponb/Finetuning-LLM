from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("opened_date").desc())
w2 = Window.partitionBy("account_type").orderBy("balance", "id")
w3 = Window.partitionBy("customer_id")
w4 = Window.partitionBy("status").orderBy(F.col("customer_id_count_balance").desc_nulls_first(), "id")

valid = (
    df_accounts
    .filter(F.col("customer_id").isNotNull())
    .select(
        "*",
        F.when(F.col("balance") >= 85000, "High").when(F.col("balance") >= 21000, "Medium").otherwise("Low").alias("balance_level"),
    )
)

valid_accounts = (
    valid
    .filter((F.col("opened_date") >= F.lit("2023-04-25").cast("date")) & F.col("customer_id").isNotNull())
    .withColumn("account_type_status_key", F.concat_ws("-", "account_type", "status"))
    .withColumn("status_filled", F.coalesce("status", F.lit("NONE")))
)

ranked = (
    valid_accounts
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)

metrics = (
    ranked
    .withColumn("balance_bucket", F.ntile(3).over(w2))
    .withColumn("customer_id_count_balance", F.count("balance").over(w3))
    .withColumn("customer_id_count_balance_2", F.count("balance").over(w3))
)

latest = (
    metrics
    .withColumn("row_num_2", F.row_number().over(w4))
    .filter(F.col("row_num_2") == 1)
)

result = (
    latest
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "inner")
    .groupBy(
        F.col("p.segment").alias("segment"),
        F.col("p.country").alias("country"),
        F.col("a.account_type").alias("account_type"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.max(F.col("a.balance")).alias("max_balance"),
    )
)
