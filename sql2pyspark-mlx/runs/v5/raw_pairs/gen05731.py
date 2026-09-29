from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating", "id")
w_part = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())
w4 = Window.partitionBy("city").orderBy(F.col("product_cost").asc_nulls_last(), "id")
w5 = Window.orderBy("product_stock_qty", "id")
w6 = Window.partitionBy("city").orderBy("product_cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("city").orderBy("product_cost", "id").rowsBetween(-3, Window.currentRow)

cleaned = (
    df_suppliers
    .filter(F.col("supplier_name").isNotNull())
    .select(
        "id",
        "supplier_name",
        "city",
        "rating",
        F.concat_ws("-", "country", "supplier_name").alias("country_supplier_name_key"),
        F.coalesce("city", F.lit("OTHER")).alias("resolved_city"),
    )
)

metrics = (
    cleaned
    .select(
        "id",
        "supplier_name",
        "city",
        "country_supplier_name_key",
        "resolved_city",
        F.lag("rating", 2).over(w).alias("prev_rating"),
        F.last("rating", ignorenulls=True).over(w_part).alias("last_known_rating"),
    )
)

cleaned_suppliers = (
    metrics
    .filter(F.upper("supplier_name").like("%ACI%"))
    .select(
        "id",
        "supplier_name",
        "city",
        "country_supplier_name_key",
        "resolved_city",
        "prev_rating",
        F.concat("city", F.lit(" "), "supplier_name").alias("city_supplier_name"),
        F.coalesce("city", F.lit("NONE")).alias("city_clean"),
    )
)

latest_match = (
    cleaned_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.supplier_name",
        "s.city",
        "s.country_supplier_name_key",
        "s.prev_rating",
        "s.city_supplier_name",
        "s.city_clean",
        F.col("c.stock_qty").alias("product_stock_qty"),
        F.col("c.cost").alias("product_cost"),
    )
)

base = (
    latest_match
    .filter(F.col("city").isNotNull())
    .select(
        "*",
        F.concat_ws("/", "supplier_name", "city").alias("supplier_name_city_key"),
        F.coalesce("city", F.lit("NONE")).alias("city_filled"),
    )
)

latest = (
    base
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
)

enriched = (
    latest
    .select(
        "*",
        F.lag("product_stock_qty", 1).over(w5).alias("prev_product_stock_qty"),
        F.sum("product_cost").over(w6).alias("running_product_cost"),
        F.avg("product_stock_qty").over(w7).alias("product_stock_qty_avg_3"),
    )
)

result = (
    enriched
    .select(
        "id",
        "city",
        "country_supplier_name_key",
        "prev_rating",
        "city_supplier_name",
        "city_clean",
        "supplier_name_city_key",
        "city_filled",
        "rn",
        F.when(F.col("city").rlike("^(Singapore|Bangkok)$"), "EXTENDED").when(F.col("city").rlike("^(Chiang Mai|New York)$"), "GROUP_B").otherwise("PRIORITY").alias("city_zone"),
    )
    .orderBy(F.col("rn").desc())
)
