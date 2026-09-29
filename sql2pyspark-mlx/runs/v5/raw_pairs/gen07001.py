from pyspark.sql import functions as F

result = (
    df_payments
    .select(
        "id",
        "method",
        "status",
        F.when(F.col("method") == "card", 1).otherwise(0).alias("is_card"),
    )
)
