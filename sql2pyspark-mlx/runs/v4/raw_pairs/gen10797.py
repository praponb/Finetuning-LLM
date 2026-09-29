from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("opened_date").desc_nulls_first(), "id")

filtered = (
    df_accounts
    .filter(F.col("customer_id").isNotNull() & F.col("balance").between(50000, 66000))
    .select(
        "id",
        "customer_id",
        "account_type",
        "opened_date",
        "status",
        F.substring("account_type", 1, 4).alias("account_type_prefix"),
        F.coalesce("balance", F.lit(0)).alias("balance_filled"),
    )
)

result = (
    filtered
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)
