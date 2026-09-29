from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("balance_int", "id")

cleaned = (
    df_accounts
    .filter((F.col("opened_date") >= F.lit("2024-04-03").cast("date")) & F.col("status").isNotNull())
    .withColumn("balance_int", F.col("balance").cast("int"))
)

cleaned_accounts = (
    cleaned
    .filter((F.col("opened_date") > F.to_date(F.lit("2024-01-01"))) & F.col("status").isNotNull() & F.col("customer_id").isNotNull())
    .select(
        "*",
        F.floor("balance_int").alias("balance_int_whole"),
        F.add_months("opened_date", 6).alias("opened_date_plus_months"),
        F.coalesce("status", F.lit("OTHER")).alias("resolved_status"),
    )
)

deduped = (
    cleaned_accounts
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

result = (
    deduped
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "status",
        "balance_int",
        "balance_int_whole",
        "opened_date_plus_months",
        "row_num",
    )
    .orderBy("balance_int")
)
