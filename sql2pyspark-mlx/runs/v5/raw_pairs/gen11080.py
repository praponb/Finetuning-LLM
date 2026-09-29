from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("name").isNotNull() & F.col("segment").isin(["Consumer", "SMB"]) & (F.col("city") == "Tokyo"))
    .select(
        "id",
        "country",
        "segment",
        F.when(F.col("is_active"), "Yes").otherwise("No").alias("is_active_label"),
    )
)
