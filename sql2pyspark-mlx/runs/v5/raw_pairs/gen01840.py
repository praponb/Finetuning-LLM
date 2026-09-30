from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") > 3.8)
    .select("id", "supplier_name", "city", F.round("rating", 1).alias("rating_rounded"))
    .orderBy(F.col("rating_rounded").desc(), "id")
    .limit(1)
)
