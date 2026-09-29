from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("segment") == "SMB")
    .select(
        "id",
        "name",
        "segment",
        F.coalesce("email", F.lit("unassigned")).alias("email_filled"),
    )
)
