from pyspark.sql import functions as F

result = (
    df_suppliers
    .select(
        "id",
        "city",
        "supplier_name",
        F.initcap(F.trim("city")).alias("city_title"),
        F.coalesce("city", F.lit("unassigned")).alias("city_filled"),
    )
)
