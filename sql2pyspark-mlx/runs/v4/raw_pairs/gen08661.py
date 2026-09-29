from pyspark.sql import functions as F

kept = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.supplier_id").isin([10, 12, 25])), "left_anti")
)

result = (
    kept
    .select(
        "id",
        "supplier_name",
        "city",
        "rating",
        F.when(F.col("city").rlike("^(Tokyo|Singapore)$"), "EXTENDED").when(F.col("city").rlike("^(Bangkok|Chiang Mai)$"), "GROUP_B").otherwise("GROUP_A").alias("city_zone"),
    )
    .orderBy(F.col("city").desc())
)
