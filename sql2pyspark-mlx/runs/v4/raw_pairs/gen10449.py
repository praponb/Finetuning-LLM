from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("end_date").desc_nulls_last(), "id")

deduped = (
    df_subscriptions
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    deduped
    .rollup(
        F.when(F.col("monthly_fee") >= 224, "LARGE").otherwise("SMALL").alias("monthly_fee_tier"),
    )
    .agg(
        F.round(F.sum("monthly_fee") / F.nullif(F.count(F.when(F.col("status") == "ACTIVE", 1)), F.lit(0)), 2).alias("monthly_fee_per_active"),
        F.round(F.sum("monthly_fee") / F.nullif(F.count(F.when(F.col("status") == "EXPIRED", 1)), F.lit(0)), 2).alias("monthly_fee_per_expired"),
    )
    .filter(F.col("monthly_fee_per_active") > 0)
    .orderBy("monthly_fee_per_expired")
)
