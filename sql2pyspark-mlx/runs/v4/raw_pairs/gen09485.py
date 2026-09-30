from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country").isin(["UK", "JP"]))
    .withColumn("supplier_name_filled", F.coalesce("supplier_name", F.lit("unassigned")))
    .orderBy("rating", "id")
    .limit(10)
)
