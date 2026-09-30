from pyspark.sql import functions as F

eligible = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.cost") < 410), "left_anti")
)

result = (
    eligible
    .select(
        "id",
        "country",
        "city",
        "rating",
        F.when(F.col("country").rlike("^(UK|JP)$"), "PRIORITY").when(F.col("country").rlike("^(TH|US)$"), "CORE").otherwise("OTHER_ZONE").alias("country_zone"),
    )
    .orderBy(F.col("country").desc_nulls_first())
)
