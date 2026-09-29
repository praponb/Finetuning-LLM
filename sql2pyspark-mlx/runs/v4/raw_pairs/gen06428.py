from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())

cleaned = (
    df_suppliers
    .filter(F.col("supplier_name").like("Sia%"))
    .select(
        "id",
        "supplier_name",
        "city",
        F.lower("city").alias("city_lower"),
        F.coalesce("city", F.lit("N/A")).alias("resolved_city"),
    )
)

not_flagged = (
    cleaned
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.category").isin(["Sports", "Books", "Fashion"])), "left_semi")
)

cleaned_suppliers = (
    not_flagged
    .filter(F.col("supplier_name").isNotNull())
    .select(
        "*",
        F.concat_ws("/", "city", "supplier_name").alias("city_supplier_name_key"),
    )
)

filtered = (
    cleaned_suppliers
    .filter(F.col("city").isNotNull())
    .select(
        "*",
        F.concat("city", F.lit(", "), "supplier_name").alias("city_supplier_name"),
        F.initcap(F.trim("city")).alias("city_title"),
    )
)

with_latest = (
    filtered
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.stock_qty"))
)

result = (
    with_latest
    .select(
        "id",
        "supplier_name",
        "city_lower",
        "resolved_city",
        "city_supplier_name",
        "city_title",
        "stock_qty",
        F.when(F.col("supplier_name").rlike("^(Global Supply|Pacific Wholesale)$"), "GROUP_B").when(F.col("supplier_name").rlike("^(Siam Parts Co|Tokyo Traders)$"), "GROUP_A").otherwise("OTHER_ZONE").alias("supplier_name_zone"),
    )
    .orderBy(F.col("supplier_name_zone").asc_nulls_last())
)
