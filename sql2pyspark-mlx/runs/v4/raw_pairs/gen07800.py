from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("balance", "id")
w2 = Window.orderBy("balance", "id").rowsBetween(-2, -1)
w3 = Window.partitionBy("status", "customer_id")
w4 = Window.partitionBy("status").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_accounts
    .filter((F.col("opened_date") >= F.lit("2023-06-30").cast("date")) & F.col("balance").between(26000, 71000))
    .select("*", F.lower("account_type").alias("account_type_lower"))
)

result = (
    base
    .select(
        "*",
        F.lead("balance", 1).over(w).alias("next_balance"),
        F.avg("balance").over(w2).alias("balance_avg_2_prior"),
        F.stddev_samp("balance").over(w3).alias("status_stddev_samp_balance"),
        F.sum("balance").over(w4).alias("running_balance"),
    )
)
