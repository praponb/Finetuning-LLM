from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country") == "TH")
    .select(
        "id",
        "city",
        "supplier_name",
        F.coalesce("supplier_name", F.lit("N/A")).alias("supplier_name_filled"),
    )
)
