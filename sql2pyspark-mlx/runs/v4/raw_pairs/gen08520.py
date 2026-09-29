from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("amount") < 700) & ((F.col("amount") < 470) | F.col("method").isin(["card", "bank_transfer"])))
    .select(
        "id",
        "method",
        F.coalesce("amount", F.lit(0)).alias("amount_filled"),
        F.initcap(F.trim("status")).alias("status_title"),
    )
    .orderBy("method", F.col("status_title").desc())
)
