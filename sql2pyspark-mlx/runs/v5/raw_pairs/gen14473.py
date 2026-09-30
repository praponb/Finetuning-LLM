from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")
w2 = Window.partitionBy("status").orderBy("opened_date", "id")

base = (
    df_accounts
    .filter(~F.col("status").eqNullSafe(F.lit("OPEN")))
    .withColumn("clean_status", F.trim("status"))
    .withColumn("status_lower", F.lower("status"))
)

result = (
    base
    .select(
        "id",
        "customer_id",
        "balance",
        F.avg("balance").over(w).alias("status_avg_balance"),
        F.row_number().over(w2).alias("row_seq"),
    )
)
