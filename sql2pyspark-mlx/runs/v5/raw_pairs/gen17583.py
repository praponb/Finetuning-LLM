from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method", "status").orderBy(F.col("amount").desc_nulls_first(), "id")

ranked = (
    df_payments
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

result = (
    ranked
    .groupBy(F.date_trunc("quarter", "paid_at").alias("quarter_start"), "method")
    .agg(
        F.round(F.sum("amount") / F.nullif(F.count(F.when(F.col("method") == "cash", 1)), F.lit(0)), 2).alias("amount_per_cash"),
        F.size(F.collect_set("method")).alias("num_methods"),
    )
    .filter(F.col("amount_per_cash") > 0)
    .orderBy(F.col("num_methods").asc_nulls_last())
)
