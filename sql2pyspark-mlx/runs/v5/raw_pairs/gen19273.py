from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("city").orderBy(F.col("price").desc_nulls_last(), "id")
w3 = Window.partitionBy("city", "country").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("rating", "id")

with_last = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.price"))
)

valid = (
    with_last
    .filter(F.col("rating") <= 3.1)
    .select(
        "*",
        F.when(F.col("city").isin(["Tokyo", "Chiang Mai"]), "Priority").otherwise("Other").alias("city_group"),
    )
)

ranked = (
    valid
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "supplier_name", "country", "city", "rating", "city_group")
)

with_windows = (
    ranked
    .filter(~F.col("city").isin(["Chiang Mai", "Berlin", "New York"]))
    .withColumn("first_known_rating", F.first("rating", ignorenulls=True).over(w3))
    .withColumn("next_rating", F.lead("rating", 1).over(w4))
)

kept = (
    with_windows
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.supplier_id") == 1), "left_semi")
)

totals = (
    kept
    .groupBy(
        F.when(F.col("rating") >= 4.2, "LARGE").otherwise("SMALL").alias("rating_tier"),
    )
    .agg(
        F.avg("first_known_rating").alias("avg_first_known_rating"),
        F.count(F.when(F.col("city") == "Bangkok", 1)).alias("bangkok_rows"),
        F.avg("next_rating").alias("avg_next_rating"),
    )
)

result = (
    totals
    .select("rating_tier", "avg_first_known_rating", "avg_next_rating")
    .orderBy(F.col("avg_first_known_rating").desc_nulls_first())
)
