from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.price").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.launch_date").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("city").orderBy("rating", "id")
w4 = Window.partitionBy("product_brand").orderBy(F.col("event_seq").desc(), "id")
w5 = Window.partitionBy("city").orderBy(F.col("rating").desc())
w6 = Window.partitionBy("product_brand").orderBy("next_country_length", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_suppliers
    .filter(~F.col("city").eqNullSafe(F.lit("Berlin")))
    .select(
        "id",
        "supplier_name",
        "city",
        "rating",
        F.length("country").alias("country_length"),
        F.when(F.col("rating") >= 4.0, "High").when(F.col("rating") >= 2.5, "Medium").otherwise("Low").alias("rating_tier"),
        F.coalesce("city", F.lit("OTHER")).alias("city_clean"),
    )
)

not_flagged = (
    cleaned
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.price") > 720), "left_semi")
)

with_last = (
    not_flagged
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.supplier_name",
        "s.city",
        "s.rating",
        "s.country_length",
        "s.rating_tier",
        F.col("c.launch_date"),
    )
)

with_last_suppliers = (
    with_last
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.brand").alias("product_brand"))
)

enriched = (
    with_last_suppliers
    .withColumn("next_rating", F.lead("rating").over(w3))
    .withColumn("prev_rating", F.lag("rating").over(w3))
    .withColumn("prev_country_length", F.lag("country_length").over(w3))
    .withColumn("next_country_length", F.lead("country_length").over(w3))
    .withColumn("event_seq", F.row_number().over(w3))
)

eligible = (
    enriched
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.launch_date") < F.col("s.launch_date")), "left_anti")
)

enriched_suppliers = (
    eligible
    .select(
        "*",
        F.ntile(3).over(w4).alias("event_seq_bucket"),
        F.dense_rank().over(w5).alias("rating_dense_rank"),
        F.sum("next_country_length").over(w6).alias("running_next_country_length"),
    )
)

result = (
    enriched_suppliers
    .select(
        "id",
        "supplier_name",
        "city",
        "country_length",
        "launch_date",
        "product_brand",
        "next_rating",
        "next_country_length",
        "event_seq_bucket",
        F.when(F.col("product_brand").rlike("^(Initech|Globex)$"), "PRIORITY").when(F.col("product_brand").rlike("^(Acme|Umbrella)$"), "CORE").otherwise("GROUP_A").alias("product_brand_zone"),
    )
    .orderBy(F.col("id").desc(), F.col("next_country_length").desc())
)
