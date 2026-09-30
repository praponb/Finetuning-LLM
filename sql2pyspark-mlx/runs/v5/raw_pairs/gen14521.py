from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("status", "customer_id").orderBy("balance_int", "id")
w2 = Window.partitionBy("customer_id").orderBy(F.col("balance_int").desc(), "id")
w3 = Window.partitionBy("status").orderBy("balance_int", "id")
w4 = Window.partitionBy("account_type", "customer_id").orderBy("opened_date", "id").rowsBetween(-3, Window.currentRow)

base = (
    df_accounts
    .filter(F.col("status").isNotNull() & (F.col("opened_date") >= F.to_date(F.lit("2023-06-20"))))
    .select(
        "id",
        "customer_id",
        "account_type",
        "opened_date",
        "status",
        F.col("balance").cast("int").alias("balance_int"),
        F.concat("account_type", F.lit("/"), "status").alias("account_type_status"),
        F.coalesce("status", F.lit("NONE")).alias("status_clean"),
    )
)

result = (
    base
    .select(
        "*",
        F.row_number().over(w_running).alias("row_seq"),
        F.ntile(5).over(w2).alias("balance_int_bucket"),
        F.lead("balance_int", 2, 0).over(w3).alias("next_balance_int"),
        F.avg("balance_int").over(w4).alias("balance_int_avg_3"),
    )
)
