from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("segment") == "Corporate") & (F.col("country") != "TH"))
    .select(
        "id",
        "segment",
        "name",
        F.coalesce("email", F.lit("Unknown")).alias("email_filled"),
    )
    .orderBy("segment")
)
