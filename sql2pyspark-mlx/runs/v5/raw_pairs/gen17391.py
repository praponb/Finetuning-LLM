from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "city").orderBy(F.col("rating").asc_nulls_first())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.stock_qty").desc(), F.col("c.id").desc())
w_ordered = Window.partitionBy("category").orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("city").orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .withColumn("country_upper", F.upper("country"))
    .withColumn("city_filled", F.coalesce("city", F.lit("DIRECT")))
)

ranked = (
    filtered
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "supplier_name", "city")
)

latest_match = (
    ranked
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.category"), F.col("c.brand").alias("product_brand"))
)

eligible = (
    latest_match
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.category") == "Electronics"), "left_anti")
)

with_last = (
    eligible
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.launch_date"))
)

scored = (
    with_last
    .filter(F.col("product_brand").isin(["Initech", "Acme", "Umbrella"]))
    .select(
        "id",
        "supplier_name",
        "city",
        F.last("city", ignorenulls=True).over(w_ordered).alias("last_known_city"),
        F.last("product_brand", ignorenulls=True).over(w_ordered).alias("last_known_product_brand"),
        F.first("city", ignorenulls=True).over(w_grp).alias("first_known_city"),
    )
)

result = (
    scored
    .groupBy("city")
    .agg(
        F.count(F.when(F.col("city") == "London", 1)).alias("london_count"),
        F.count("*").alias("cnt"),
        F.count(F.when(F.col("city") == "Singapore", 1)).alias("singapore_rows"),
        F.count_if(F.col("city") == "Bangkok").alias("bangkok_events"),
        F.count(F.when(F.col("city") == "Berlin", 1)).alias("berlin_rows"),
    )
    .filter(F.col("bangkok_events") < 1000)
    .orderBy(F.col("london_count").desc())
)
