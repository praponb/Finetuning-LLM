from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "city").orderBy(F.col("supplier_name_length").desc_nulls_last(), "id")
w_grp = Window.partitionBy("country").orderBy(F.col("supplier_name_length").desc())
w_ordered = Window.partitionBy("country").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("country").orderBy(F.col("supplier_name_length").desc(), "id")
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.price").desc(), F.col("c.id").desc())

valid = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .withColumn("country_city_key", F.concat_ws("-", "country", "city"))
    .withColumn("resolved_city", F.coalesce("city", F.lit("DIRECT")))
)

valid_suppliers = (
    valid
    .filter(F.col("city") == "London")
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        "country_city_key",
        "resolved_city",
        F.length("supplier_name").alias("supplier_name_length"),
    )
)

latest = (
    valid_suppliers
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "country",
        "rating",
        "country_city_key",
        "resolved_city",
        "supplier_name_length",
    )
)

metrics = (
    latest
    .filter(F.col("rating") > 2.5)
    .select(
        "id",
        "rating",
        F.percent_rank().over(w_grp).alias("supplier_name_length_percent_rank"),
        F.first("rating", ignorenulls=True).over(w_ordered).alias("first_known_rating"),
        F.row_number().over(w4).alias("event_seq"),
    )
)

with_last = (
    metrics
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.category"), F.col("c.cost").alias("product_cost"))
)

result = (
    with_last
    .groupBy(
        F.when(F.col("product_cost") >= 710, "PREMIUM").otherwise("STANDARD").alias("product_cost_tier"),
        F.when(F.col("rating") >= 4.1, "LARGE").otherwise("SMALL").alias("rating_tier"),
    )
    .agg(
        F.round(F.sum("first_known_rating") / F.nullif(F.count(F.when(F.col("category") == "Grocery", 1)), F.lit(0)), 2).alias("first_known_rating_per_grocery"),
        F.countDistinct("category").alias("distinct_categories"),
        F.count(F.when(F.col("category") == "Sports", 1)).alias("sports_count"),
    )
)
