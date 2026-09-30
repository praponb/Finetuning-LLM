from pyspark.sql import functions as F

base = (
    df_suppliers
    .filter(F.col("supplier_name").isNull())
    .withColumn("supplier_name_country_key", F.concat_ws("/", "supplier_name", "country"))
)

result = (
    base
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.price") < 760), "left_anti")
)
