from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "account_type").orderBy("balance", "id").rowsBetween(-2, -1)
w2 = Window.partitionBy("customer_id").orderBy(F.col("opened_date").desc(), "id")
w3 = Window.partitionBy("status").orderBy(F.col("balance").asc_nulls_last())
w4 = Window.partitionBy("account_type")

cleaned = (
    df_accounts
    .filter((F.col("opened_date") > F.to_date(F.lit("2023-06-03"))) & F.col("customer_id").isNotNull())
    .withColumn("status_prefix", F.substring("status", 1, 3))
    .withColumn("resolved_status", F.coalesce("status", F.lit("NONE")))
)

result = (
    cleaned
    .select(
        "*",
        F.avg("balance").over(w).alias("balance_avg_2_prior"),
        F.row_number().over(w2).alias("row_seq"),
        F.percent_rank().over(w3).alias("balance_percent_rank"),
        F.avg("balance").over(w4).alias("account_type_avg_balance"),
    )
)
