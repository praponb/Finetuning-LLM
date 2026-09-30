from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("paid_at").desc(), "id")
w2 = Window.partitionBy("method").orderBy(F.col("amount").desc())
w3 = Window.orderBy(F.col("amount_rounded").desc_nulls_first())
w4 = Window.partitionBy("method").orderBy("rank_in_group", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_payments
    .filter(F.col("paid_at") > F.to_date(F.lit("2023-05-16")))
    .select(
        "*",
        F.round("amount", 0).alias("amount_rounded"),
        F.coalesce("method", F.lit("OTHER")).alias("resolved_method"),
    )
)

deduped = (
    cleaned
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

deduped_payments = (
    deduped
    .withColumn("rank_in_group", F.rank().over(w2))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    deduped_payments
    .select(
        "*",
        F.cume_dist().over(w3).alias("amount_rounded_cume_dist"),
        F.last("amount_rounded", ignorenulls=True).over(w4).alias("last_known_amount_rounded"),
    )
)
