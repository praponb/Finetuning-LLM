from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.stock_qty").desc(), F.col("c.id").desc())

cleaned = (
    df_suppliers
    .filter(F.col("rating") >= 4.0)
    .withColumn("is_bangkok", F.when(F.col("city") == "Bangkok", 1).otherwise(0))
)

cleaned_suppliers = (
    cleaned
    .filter(F.col("city").isNotNull() & (F.col("city") == "New York"))
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        "is_bangkok",
        F.length("country").alias("country_length"),
        F.coalesce("city", F.lit("OTHER")).alias("resolved_city"),
    )
)

with_windows = (
    cleaned_suppliers
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        "is_bangkok",
        "country_length",
        "resolved_city",
        F.sum("is_bangkok").over(w_ordered).alias("running_sum_is_bangkok"),
        F.max("is_bangkok").over(w_ordered).alias("running_max_is_bangkok"),
        F.min("is_bangkok").over(w_ordered).alias("running_min_is_bangkok"),
    )
)

with_latest = (
    with_windows
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.category"), F.col("c.brand"))
)

filtered = (
    with_latest
    .filter(F.col("category").isNotNull() & F.col("running_sum_is_bangkok").isNotNull())
    .select(
        "*",
        F.round("country_length", 1).alias("country_length_rounded"),
        (F.col("country_length") / F.nullif("running_sum_is_bangkok", F.lit(0))).alias("country_length_per_running_sum_is_bangkok"),
        F.coalesce("category", F.lit("DIRECT")).alias("category_filled"),
    )
)

result = (
    filtered
    .select(
        "id",
        "supplier_name",
        "country",
        "rating",
        "is_bangkok",
        "country_length",
        "resolved_city",
        "running_sum_is_bangkok",
        "running_max_is_bangkok",
    )
    .orderBy(F.col("rating").desc())
)
