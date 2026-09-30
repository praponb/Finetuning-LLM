from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("amount", "id")

latest = (
    df_payments
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

result = (
    latest
    .cube(
        "status",
        F.when(F.col("amount") >= 1580, "HIGH").otherwise("LOW").alias("amount_tier"),
    )
    .agg(
        F.percentile_approx("amount", [0.25, 0.5, 0.75]).alias("amount_quartiles"),
        F.stddev_samp("amount").alias("amount_stddev"),
        F.size(F.collect_set("method")).alias("num_methods"),
        F.percentile_approx("amount", [0.25, 0.5, 0.75]).alias("amount_quartiles_2"),
        F.sum(F.when(F.col("method") == "cash", F.col("amount")).otherwise(0)).alias("cash_amount"),
        F.max("paid_at").alias("last_paid_at"),
    )
    .filter(F.col("cash_amount") < 0)
    .orderBy("status")
)
