from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("country").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("country")
w_hist = Window.partitionBy("country").orderBy("last3_max_rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())
w6 = Window.orderBy(F.col("country_sum_last3_avg_rating").asc_nulls_last())
w7 = Window.orderBy("running_last3_count_rating", "id").rowsBetween(-3, -1)
w8 = Window.orderBy(F.col("last3_max_rating").desc_nulls_last())
w9 = Window.orderBy("running_last3_count_rating", "id").rowsBetween(-5, Window.currentRow)

cleaned = (
    df_suppliers
    .filter((~F.col("city").eqNullSafe(F.lit("London"))) & (F.col("city") == "Bangkok"))
    .withColumn("rating_whole", F.floor("rating"))
    .withColumn("supplier_name_lower", F.lower("supplier_name"))
    .withColumn("city_filled", F.coalesce("city", F.lit("UNKNOWN")))
)

with_windows = (
    cleaned
    .select(
        "id",
        "country",
        "supplier_name_lower",
        "city_filled",
        F.sum("rating").over(w).alias("last3_sum_rating"),
        F.avg("rating").over(w).alias("last3_avg_rating"),
        F.count("rating").over(w).alias("last3_count_rating"),
        F.max("rating").over(w).alias("last3_max_rating"),
        F.first("city", ignorenulls=True).over(w2).alias("first_known_city"),
    )
)

enriched = (
    with_windows
    .filter(~F.col("country").isin(["SG", "UK", "TH"]))
    .select(
        "id",
        "country",
        "last3_avg_rating",
        "last3_max_rating",
        F.sum("last3_avg_rating").over(w3).alias("country_sum_last3_avg_rating"),
        F.sum("last3_count_rating").over(w_hist).alias("running_last3_count_rating"),
    )
)

with_latest = (
    enriched
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.last3_avg_rating",
        "s.last3_max_rating",
        "s.country_sum_last3_avg_rating",
        "s.running_last3_count_rating",
        F.col("c.cost"),
    )
)

filtered = (
    with_latest
    .filter(F.col("country_sum_last3_avg_rating") >= 8)
    .select(
        "id",
        "last3_max_rating",
        "country_sum_last3_avg_rating",
        "running_last3_count_rating",
        "cost",
        F.col("last3_max_rating").cast("int").alias("last3_max_rating_int"),
    )
)

result = (
    filtered
    .select(
        "id",
        "last3_max_rating",
        "last3_max_rating_int",
        F.rank().over(w6).alias("country_sum_last3_avg_rating_rank"),
        F.avg("country_sum_last3_avg_rating").over(w7).alias("country_sum_last3_avg_rating_avg_3_prior"),
        F.cume_dist().over(w8).alias("last3_max_rating_cume_dist"),
        F.avg("cost").over(w9).alias("cost_avg_5"),
    )
)
