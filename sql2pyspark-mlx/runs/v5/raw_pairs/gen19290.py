from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.lower("supplier_name").like("%iam%") & F.lower("supplier_name").like("%lob%"))
    .select(
        "id",
        "country",
        "city",
        F.regexp_replace("supplier_name", " ", "_").alias("supplier_name_slug"),
    )
)
