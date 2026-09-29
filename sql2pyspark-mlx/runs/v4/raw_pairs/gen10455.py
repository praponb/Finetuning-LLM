from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type", "customer_id").orderBy(F.col("opened_date").asc_nulls_last(), "id")

filtered = (
    df_accounts
    .filter((F.col("opened_date") > F.to_date(F.lit("2023-06-13"))) & F.col("customer_id").isNotNull())
    .select(
        "*",
        F.round("balance", 0).alias("balance_rounded"),
        F.coalesce("status", F.lit("NONE")).alias("resolved_status"),
    )
)

result = (
    filtered
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)
