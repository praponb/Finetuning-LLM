from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("city").orderBy("product_launch_date", "id")
w3 = Window.partitionBy("country")
w4 = Window.partitionBy("country").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("city", "country").orderBy("product_cost", "id")

base = (
    df_suppliers
    .filter((F.col("city").eqNullSafe(F.lit("Chiang Mai"))) & (F.col("rating") > 2.6))
    .withColumn("country_city_key", F.concat_ws(",", "country", "city"))
)

latest_match = (
    base
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.cost").alias("product_cost"),
        F.col("c.launch_date").alias("product_launch_date"),
    )
)

scored = (
    latest_match
    .select(
        "*",
        F.row_number().over(w2).alias("event_seq"),
        F.count("product_cost").over(w3).alias("country_count_product_cost"),
        F.sum("rating").over(w4).alias("cumulative_rating"),
    )
)

with_windows = (
    scored
    .select(
        "id",
        "supplier_name",
        "country",
        "rating",
        "country_city_key",
        "product_launch_date",
        "cumulative_rating",
        F.lead("cumulative_rating").over(w_grp).alias("next_cumulative_rating"),
        F.lag("cumulative_rating").over(w_grp).alias("prev_cumulative_rating"),
        F.lead("rating").over(w_grp).alias("next_rating"),
        F.row_number().over(w_grp).alias("event_seq_2"),
    )
)

eligible = (
    with_windows
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.launch_date") < F.col("s.product_launch_date")), "left_semi")
)

with_last = (
    eligible
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.stock_qty").alias("product_stock_qty"))
)

result = (
    with_last
    .select(
        "id",
        "supplier_name",
        "product_launch_date",
        "next_cumulative_rating",
        "prev_cumulative_rating",
        "next_rating",
        "event_seq_2",
        "product_stock_qty",
    )
)
