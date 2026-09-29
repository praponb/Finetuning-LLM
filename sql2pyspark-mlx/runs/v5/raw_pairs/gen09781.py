from pyspark.sql import functions as F

result = (
    df_suppliers
    .select(
        "id",
        "country",
        F.concat("city", F.lit("/"), "country").alias("city_country"),
        F.coalesce("supplier_name", F.lit("unassigned")).alias("supplier_name_filled"),
    )
)
