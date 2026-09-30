from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("paid_at").desc_nulls_last(), "id")

base = (
    df_payments
    .filter(F.col("method").isNotNull() & (F.col("method").eqNullSafe(F.lit("card"))))
    .select(
        "id",
        "paid_at",
        "status",
        F.concat("status", F.lit(" - "), "method").alias("status_method"),
        F.when(F.col("status").isin(["REFUNDED", "SUCCESS"]), "Group A").otherwise("Other").alias("status_group"),
        F.coalesce("method", F.lit("UNKNOWN")).alias("method_filled"),
    )
)

result = (
    base
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "paid_at", "status", "status_method", "status_group", "method_filled")
)
