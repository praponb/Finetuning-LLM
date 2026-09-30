from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.stock_qty").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("city").orderBy("rating", "id")
w3 = Window.partitionBy("brand").orderBy("rating", "id")

valid = (
    df_suppliers
    .filter(F.col("city").isin(["Singapore", "Berlin", "Bangkok"]))
    .select(
        "*",
        F.upper(F.trim("supplier_name")).alias("supplier_name_normalized"),
        F.coalesce("city", F.lit("UNKNOWN")).alias("resolved_city"),
    )
)

latest_match = (
    valid
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.brand"))
)

kept = (
    latest_match
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.cost") > 670), "left_semi")
)

not_flagged = (
    kept
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.stock_qty") > 233), "left_semi")
)

metrics = (
    not_flagged
    .withColumn("prev_rating", F.lag("rating").over(w2))
    .withColumn("seq_no", F.row_number().over(w2))
)

result = (
    metrics
    .withColumn("next_rating", F.lead("rating").over(w3))
    .withColumn("prev_rating_2", F.lag("rating").over(w3))
    .withColumn("prev_prev_rating", F.lag("prev_rating").over(w3))
    .withColumn("next_prev_rating", F.lead("prev_rating").over(w3))
    .withColumn("row_seq", F.row_number().over(w3))
)
