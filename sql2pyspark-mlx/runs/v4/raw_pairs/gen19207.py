from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("amount", "id")
w2 = Window.partitionBy("method").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("method").orderBy(F.col("amount").desc(), "id")

cleaned = (
    df_payments
    .filter(F.col("paid_at") > F.to_date(F.lit("2023-08-18")))
    .select(
        "id",
        "method",
        "amount",
        F.when(F.col("status").isin(["FAILED", "REFUNDED"]), "Priority").otherwise("Other").alias("status_group"),
        F.coalesce("method", F.lit("NONE")).alias("method_clean"),
    )
)

result = (
    cleaned
    .withColumn("next_amount", F.lead("amount", 2).over(w))
    .withColumn("running_amount", F.sum("amount").over(w2))
    .withColumn("seq_no", F.row_number().over(w3))
    .withColumn("next_amount_2", F.lead("amount", 2).over(w))
)
