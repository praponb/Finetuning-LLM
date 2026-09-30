from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("amount").desc(), "id")
w2 = Window.partitionBy("account_id").orderBy(F.col("amount").desc(), "id")
w3 = Window.partitionBy("account_id").orderBy("amount", "id").rowsBetween(-2, Window.currentRow)
w4 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(-6, -1)

enriched = (
    df_transactions
    .withColumn("amount_bucket", F.ntile(10).over(w))
    .withColumn("amount_bucket_2", F.ntile(10).over(w2))
    .withColumn("amount_avg_2", F.avg("amount").over(w3))
    .withColumn("amount_avg_6_prior", F.avg("amount").over(w4))
)

result = (
    enriched
    .groupBy(
        F.date_trunc("QUARTER", "txn_time").alias("quarter_start"),
        F.when(F.col("amount_bucket_2") >= 8, "LARGE").otherwise("SMALL").alias("amount_bucket_2_tier"),
        F.date_trunc("MONTH", "txn_time").alias("activity_month"),
    )
    .agg(
        ((F.unix_timestamp(F.max("txn_time")) - F.unix_timestamp(F.min("txn_time"))) / 60).alias("duration_min"),
        F.size(F.collect_set("merchant")).alias("num_merchants"),
    )
    .orderBy(F.col("activity_month").desc_nulls_first())
)
