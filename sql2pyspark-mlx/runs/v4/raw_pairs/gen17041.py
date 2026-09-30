from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.stock_qty").desc(), F.col("c.id").desc())

cleaned = (
    df_suppliers
    .filter(F.col("city").isNotNull() & F.col("country").isin(["JP", "UK"]))
    .select(
        "id",
        "supplier_name",
        "city",
        "rating",
        F.concat_ws(",", "supplier_name", "country").alias("supplier_name_country_key"),
        F.coalesce("city", F.lit("NONE")).alias("city_clean"),
    )
)

base = (
    cleaned
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "city",
        "supplier_name_country_key",
        "city_clean",
        F.when(F.col("rating") >= 4.3, "High").when(F.col("rating") >= 2.1, "Medium").otherwise("Low").alias("rating_band"),
        F.upper("city").alias("city_upper"),
    )
)

latest_match = (
    base
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.category"))
)

daily = (
    latest_match
    .groupBy("city", "category")
    .agg(
        F.size(F.collect_set("category")).alias("num_categories"),
        F.sort_array(F.collect_set("category")).alias("category_list"),
        F.countDistinct("category").alias("distinct_categories"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)

result = (
    daily
    .select(
        "city",
        "category",
        "num_categories",
        "distinct_categories",
        "distinct_ids",
        F.when(F.col("city").rlike("^(London|Singapore)$"), "EXTENDED").when(F.col("city").rlike("^(Bangkok|Chiang Mai)$"), "CORE").otherwise("PRIORITY").alias("city_zone"),
    )
    .orderBy(F.col("city_zone").desc())
)
