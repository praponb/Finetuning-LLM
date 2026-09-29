from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating", "id")

base = (
    df_suppliers
    .filter(~F.col("country").isin(["TH", "UK", "DE"]))
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        F.when(F.col("country") == "TH", 1).otherwise(0).alias("is_th"),
    )
)

valid = (
    base
    .filter(F.col("city").isNotNull() & F.col("supplier_name").like("%Paci%"))
    .select(
        "id",
        "country",
        "city",
        "rating",
        F.trim("city").alias("clean_city"),
        F.coalesce("city", F.lit("N/A")).alias("resolved_city"),
    )
)

latest = (
    valid
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "city")
)

not_flagged = (
    latest
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.month(F.col("ch.launch_date")).isin([7, 8, 10])), "left_anti")
)

result = (
    not_flagged
    .groupBy("city")
    .agg(
        F.count_if(F.col("city") == "New York").alias("new_york_events"),
        F.count("*").alias("row_count"),
        F.count_if(F.col("city") == "Bangkok").alias("bangkok_events"),
        F.size(F.collect_set("city")).alias("num_cities"),
    )
    .filter(F.col("num_cities") < 1000)
    .orderBy(F.col("new_york_events").asc_nulls_last())
)
