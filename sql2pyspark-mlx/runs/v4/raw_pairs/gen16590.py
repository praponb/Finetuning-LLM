from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy(F.col("opened_date_period").desc(), "id")

cleaned = (
    df_accounts
    .filter((F.col("opened_date") > F.to_date(F.lit("2023-08-19"))) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        F.date_trunc("WEEK", "opened_date").alias("opened_date_period"),
        F.dayofweek("opened_date").alias("opened_date_dayofweek"),
        F.coalesce("status", F.lit("NONE")).alias("resolved_status"),
    )
)

result = (
    cleaned
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)
