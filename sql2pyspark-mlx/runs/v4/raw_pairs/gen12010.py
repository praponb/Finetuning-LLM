from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.price").desc(), F.col("c.id").desc())
w_part = Window.partitionBy("country").orderBy("country_length", "id")
w_running = Window.partitionBy("country", "product_category").orderBy("country_length_rounded", "id").rowsBetween(-2, -1)
w4 = Window.partitionBy("product_category").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("country", "city").orderBy("rating", "id")
w6 = Window.partitionBy("city").orderBy("country_length_rounded", "id").rowsBetween(-6, Window.currentRow)

cleaned = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        F.length("country").alias("country_length"),
    )
)

base = (
    cleaned
    .filter(F.col("city") == "Chiang Mai")
    .withColumn("country_length_rounded", F.round("country_length", 2))
    .withColumn("city_filled", F.coalesce("city", F.lit("DIRECT")))
)

with_last = (
    base
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.category").alias("product_category"))
)

scored = (
    with_last
    .select(
        "*",
        F.lag("country_length_rounded", 1).over(w_part).alias("prev_country_length_rounded"),
        F.avg("rating").over(w_running).alias("rating_avg_2_prior"),
        F.sum("country_length_rounded").over(w4).alias("running_country_length_rounded"),
        F.lead("country_length_rounded", 1).over(w5).alias("next_country_length_rounded"),
        F.avg("country_length_rounded").over(w6).alias("country_length_rounded_avg_6"),
    )
)

result = (
    scored
    .groupBy("product_category")
    .pivot("city", ["Berlin", "Tokyo"])
    .agg(F.avg("running_country_length_rounded").alias("total"))
)
