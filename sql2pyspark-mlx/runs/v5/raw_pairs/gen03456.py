from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("amount", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("method").orderBy(F.col("amount").asc_nulls_last())

valid = (
    df_payments
    .filter(F.col("method").isNotNull())
    .select(
        "*",
        F.col("amount").cast("int").alias("amount_int"),
        F.when(F.col("method").isin(["card", "bank_transfer"]), "Tier 1").otherwise("Other").alias("method_group"),
        F.coalesce("method", F.lit("NONE")).alias("method_clean"),
    )
)

result = (
    valid
    .select(
        "*",
        F.min("amount_int").over(w).alias("last3_min_amount_int"),
        F.avg("amount_int").over(w).alias("last3_avg_amount_int"),
        F.count("amount_int").over(w).alias("last3_count_amount_int"),
        F.cume_dist().over(w2).alias("amount_cume_dist"),
    )
)
