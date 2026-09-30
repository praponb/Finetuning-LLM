from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.balance").desc(), F.col("c.id").desc())

with_last = (
    df_customers
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.account_type"))
)

result = (
    with_last
    .withColumn("year_start", F.date_trunc("year", "signup_date"))
    .withColumn("credit_limit_tier", F.when(F.col("credit_limit") >= 13700, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["account_type", "year_start", "credit_limit_tier"], ["account_type", "year_start"], []],
        "account_type",
        "year_start",
        "credit_limit_tier",
    )
    .agg(
        F.count("*").alias("event_count"),
        F.size(F.collect_set("account_type")).alias("num_account_types"),
        F.size(F.collect_set("account_type")).alias("num_account_types_2"),
        F.grouping_id().alias("grouping_level"),
    )
)
