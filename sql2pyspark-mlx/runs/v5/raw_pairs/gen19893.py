from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("shipped_date").between("2024-01-09", "2024-10-01"))
    .select(
        "id",
        "shipped_date",
        F.when(F.col("status") == "LOST", 1).otherwise(0).alias("is_lost"),
    )
    .orderBy(F.col("is_lost").desc())
)
