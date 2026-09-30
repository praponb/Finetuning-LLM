from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("country_length").desc_nulls_first(), "id")
w2 = Window.partitionBy("city", "country").orderBy(F.col("rating").desc())
w_ordered = Window.partitionBy("city").orderBy("country_length", "id")
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.stock_qty").desc(), F.col("c.id").desc())

filtered = (
    df_suppliers
    .filter(F.col("rating") >= 3.6)
    .select("*", F.length("country").alias("country_length"))
)

ranked = (
    filtered
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

ranked_suppliers = (
    ranked
    .withColumn("latest_rank", F.rank().over(w2))
    .filter(F.col("latest_rank") == 1)
)

with_windows = (
    ranked_suppliers
    .select(
        "*",
        F.lead("rating").over(w_ordered).alias("next_rating"),
        F.lag("country_length").over(w_ordered).alias("prev_country_length"),
        F.row_number().over(w_ordered).alias("seq_no"),
    )
)

kept = (
    with_windows
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.cost") < 470), "left_semi")
)

latest_match = (
    kept
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.supplier_name",
        "s.country",
        "s.country_length",
        "s.latest_rank",
        F.col("c.stock_qty"),
    )
)

result = latest_match.select("id", "country_length", "latest_rank")
