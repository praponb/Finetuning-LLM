from pyspark.sql import functions as F

valid = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select(
        "*",
        F.when(F.col("country").isin(["UK", "SG"]), "Group A").otherwise("Other").alias("country_group"),
    )
)

result = (
    valid
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.supplier_id") == 17), "left_anti")
)
