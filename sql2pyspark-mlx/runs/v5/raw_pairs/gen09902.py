from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city").isin(["Tokyo", "New York"]) & ((F.col("city") == "Tokyo") | (F.col("city") == "London")))
    .select(
        "supplier_name",
        F.when(F.col("country").isin(["TH", "US"]), "Tier 1").otherwise("Other").alias("country_group"),
    )
    .orderBy(F.col("supplier_name").desc())
)
