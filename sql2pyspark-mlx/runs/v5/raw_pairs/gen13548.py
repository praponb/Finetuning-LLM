from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("city").isin(["New York", "London"]))
    .select(F.lit("customer").alias("source"), "city", "country")
    .unionByName(
        df_suppliers
        .select(F.lit("supplier").alias("source"), "city", "country")
    )
)
