from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())

with_last = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.launch_date"))
)

result = (
    with_last
    .groupBy(
        F.when(F.col("rating") >= 3.5, "LARGE").otherwise("SMALL").alias("rating_tier"),
        "city",
    )
    .agg(
        F.percentile_approx("rating", 0.9).alias("rating_p90"),
        F.max(F.when(F.col("country") == "DE", F.col("rating"))).alias("de_max_rating"),
        F.percentile_approx("rating", [0.25, 0.5, 0.75]).alias("rating_quartiles"),
        F.sum(F.when(F.col("city") == "New York", F.col("rating")).otherwise(0)).alias("new_york_rating"),
    )
    .orderBy(F.col("rating_p90").asc_nulls_first())
)
