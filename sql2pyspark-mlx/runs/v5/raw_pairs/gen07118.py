from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.length("supplier_name") < 12) & (F.lower(F.trim("supplier_name")) == "pacific wholesale"))
    .select(
        "id",
        "city",
        "country",
        F.initcap(F.trim("supplier_name")).alias("supplier_name_title"),
        F.when(F.col("country").isin(["SG", "US"]), "Priority").otherwise("Other").alias("country_group"),
    )
    .orderBy(F.col("id").desc(), "country")
)
