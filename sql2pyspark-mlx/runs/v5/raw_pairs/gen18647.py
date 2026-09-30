from pyspark.sql import functions as F

result = (
    df_customers
    .select(F.lit("customer").alias("record_type"), "country", "city")
    .unionByName(
        df_suppliers
        .filter(F.col("supplier_name").isNull())
        .select(F.lit("supplier").alias("record_type"), "country", "city")
    )
    .distinct()
    .orderBy(F.col("city").desc())
)
