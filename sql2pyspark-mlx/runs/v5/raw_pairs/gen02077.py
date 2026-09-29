from pyspark.sql import functions as F

result = (
    df_customers
    .select("country", "city", F.lit("customer").alias("source"))
    .unionByName(
        df_suppliers
        .filter(F.col("supplier_name").like("Gl%"))
        .select("country", "city", F.lit("supplier").alias("source"))
    )
)
