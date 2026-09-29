from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "city").orderBy("rating", "id")

filtered = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .withColumn("city_group", F.when(F.col("city").isin(["Tokyo", "Singapore"]), "Group A").otherwise("Other"))
    .withColumn("city_filled", F.coalesce("city", F.lit("N/A")))
)

latest = (
    filtered
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

eligible = (
    latest
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.category") == "Home"), "left_anti")
)

filtered_suppliers = (
    eligible
    .filter(F.col("rank_in_group") <= 1)
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        "city_group",
        "city_filled",
        "rank_in_group",
        F.trim("supplier_name").alias("clean_supplier_name"),
        F.regexp_replace("supplier_name", " ", "").alias("supplier_name_slug"),
        F.coalesce("city", F.lit("DIRECT")).alias("city_filled_2"),
    )
)

eligible_suppliers = (
    filtered_suppliers
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.upper(F.trim(F.col("ch.product_name"))) == "OFFICE CHAIR"), "left_semi")
)

result = (
    eligible_suppliers
    .select(
        "id",
        "supplier_name",
        "city",
        "rating",
        "city_group",
        "city_filled",
        "rank_in_group",
        "clean_supplier_name",
    )
    .orderBy(F.col("city").desc_nulls_last(), "rank_in_group")
)
