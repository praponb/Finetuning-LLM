from pyspark.sql import functions as F

not_flagged = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.stock_qty") > 377), "left_anti")
)

result = (
    not_flagged
    .select(
        "id",
        "supplier_name",
        "country",
        "rating",
        F.when(F.col("country").rlike("^(TH|US)$"), "PRIORITY").otherwise("CORE").alias("country_zone"),
    )
    .orderBy(F.col("country_zone").desc())
)
