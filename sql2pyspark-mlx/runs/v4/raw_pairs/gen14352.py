from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.launch_date").desc(), F.col("c.id").desc())

base = (
    df_suppliers
    .filter(F.col("supplier_name").isNotNull())
    .select(
        "*",
        F.when(F.col("city") == "Berlin", 1).otherwise(0).alias("is_berlin"),
        F.coalesce("city", F.lit("N/A")).alias("city_clean"),
    )
)

cleaned = (
    base
    .filter(F.col("supplier_name").like("E%"))
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "city_clean",
        F.upper("supplier_name").alias("supplier_name_upper"),
    )
)

with_latest = (
    cleaned
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.category").alias("product_category"), F.col("c.brand"))
)

base_suppliers = (
    with_latest
    .filter(F.col("city").isNotNull())
    .withColumn("city_brand_key", F.concat_ws(",", "city", "brand"))
    .withColumn("product_category_group", F.when(F.col("product_category").isin(["Electronics", "Sports"]), "Priority").otherwise("Other"))
)

result = (
    base_suppliers
    .select(
        "id",
        "supplier_name",
        "product_category",
        "brand",
        "city_brand_key",
        "product_category_group",
        F.when(F.col("product_category").rlike("^(Sports|Grocery)$"), "OTHER_ZONE").otherwise("GROUP_A").alias("product_category_zone"),
    )
    .orderBy(F.col("brand").desc(), "product_category")
)
