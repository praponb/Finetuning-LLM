from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "status").orderBy(F.col("opened_date").desc(), "id")

base = (
    df_accounts
    .filter(F.col("opened_date") > F.lit("2023-06-01").cast("date"))
    .withColumn("status_filled", F.coalesce("status", F.lit("unassigned")))
    .withColumn("balance_rounded", F.round("balance", 1))
    .withColumn("resolved_status", F.coalesce("status", F.lit("NONE")))
)

result = (
    base
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)
