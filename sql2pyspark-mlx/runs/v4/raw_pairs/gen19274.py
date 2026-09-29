from pyspark.sql import functions as F

result = (
    df_suppliers
    .select(
        "supplier_name",
        "rating",
        "country",
        F.coalesce("supplier_name", F.lit("unassigned")).alias("supplier_name_filled"),
        F.regexp_replace("supplier_name", " ", "").alias("supplier_name_slug"),
    )
)
