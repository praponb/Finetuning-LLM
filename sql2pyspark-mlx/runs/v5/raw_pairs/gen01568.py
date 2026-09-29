from pyspark.sql import functions as F

result = (
    df_suppliers
    .select(
        "city",
        "supplier_name",
        "country",
        F.coalesce("city", F.lit("unassigned")).alias("city_filled"),
        F.when(F.col("city").isin(["Tokyo", "Singapore"]), "Tier 1").otherwise("Other").alias("city_group"),
    )
    .orderBy(F.col("country").desc())
)
