from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("amount_p95").desc())

result = (
    df_payments
    .groupBy("status", "method")
    .agg(
        F.round(F.sum("amount") / F.nullif(F.count(F.when(F.col("status") == "SUCCESS", 1)), F.lit(0)), 2).alias("amount_per_success"),
        F.percentile_approx("amount", 0.95).alias("amount_p95"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
    .filter(F.col("amount_p95") >= 0)
    .withColumn("amount_p95_rank", F.rank().over(w))
)
