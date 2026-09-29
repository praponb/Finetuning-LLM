from pyspark.sql import functions as F

result = (
    df_suppliers
    .select(
        "id",
        "rating",
        F.when(F.col("rating") >= 3.5, "High").when(F.col("rating") >= 1.9, "Medium").otherwise("Low").alias("rating_tier"),
        F.coalesce("supplier_name", F.lit("unassigned")).alias("supplier_name_filled"),
    )
)
