from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("account_type").orderBy(F.col("opened_date").desc(), "id")
w2 = Window.partitionBy("status").orderBy("opened_date", "id")

valid = (
    df_accounts
    .filter((F.col("opened_date") >= F.to_date(F.lit("2024-04-29"))) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "account_type",
        "opened_date",
        "status",
        F.upper("account_type").alias("account_type_upper"),
        F.coalesce("status", F.lit("NONE")).alias("resolved_status"),
    )
)

result = (
    valid
    .withColumn("seq_no", F.row_number().over(w_running))
    .withColumn("event_seq", F.row_number().over(w2))
)
