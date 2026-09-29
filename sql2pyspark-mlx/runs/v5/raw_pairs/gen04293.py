from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy(F.col("rating").desc_nulls_first(), "id")

base = (
    df_products
    .filter(F.col("supplier_id").isNotNull())
    .withColumn("category_upper", F.upper("category"))
    .withColumn("brand_clean", F.coalesce("brand", F.lit("N/A")))
)

enriched = (
    base
    .alias("a")
    .join(df_suppliers.alias("p"), F.col("a.supplier_id") == F.col("p.id"), "inner")
    .select(
        "a.id",
        "a.product_name",
        "a.brand",
        "a.tags",
        "a.supplier_id",
        F.col("p.rating"),
    )
)

not_flagged = (
    enriched
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.revenue") > 1800), "left_anti")
)

valid = (
    not_flagged
    .filter(F.col("supplier_id").isNotNull() & (F.col("brand").eqNullSafe(F.lit("Initech"))))
    .select(
        "id",
        "product_name",
        "tags",
        "supplier_id",
        "rating",
        F.when(F.col("rating") >= 3.8, "High").when(F.col("rating") >= 2.2, "Medium").otherwise("Low").alias("rating_band"),
    )
)

latest = (
    valid
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

joined = (
    latest
    .alias("src")
    .join(df_suppliers.alias("s"), F.col("src.supplier_id") == F.col("s.id"), "left")
    .select(
        "src.id",
        "src.product_name",
        "src.supplier_id",
        "src.rating_band",
        F.col("s.supplier_name"),
        F.col("s.city"),
    )
)

result = (
    joined
    .groupBy("city")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("city").alias("distinct_cities"),
        F.count("*").alias("row_count"),
    )
    .orderBy("distinct_cities")
)
