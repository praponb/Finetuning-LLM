from pyspark.sql import functions as F

kept = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.cost").between(490, 580)), "left_anti")
)

result = kept.select("id", "supplier_name", "country", "city")
