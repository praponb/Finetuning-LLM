from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("city").orderBy("rating", "id")
w2 = Window.partitionBy("city", "country").orderBy(F.col("next_rating").desc_nulls_first(), "id")

base = (
    df_suppliers
    .filter(~F.col("city").eqNullSafe(F.lit("Chiang Mai")))
    .select(
        "*",
        F.concat("supplier_name", F.lit(" - "), "city").alias("supplier_name_city"),
        F.upper("city").alias("city_upper"),
    )
)

eligible = (
    base
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.product_name").like("%Backp%")), "left_semi")
)

eligible_suppliers = (
    eligible
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.stock_qty").between(106, 175)), "left_anti")
)

scored = (
    eligible_suppliers
    .filter(F.col("rating").isNull())
    .withColumn("next_rating", F.lead("rating").over(w_running))
    .withColumn("prev_rating", F.lag("rating").over(w_running))
    .withColumn("event_seq", F.row_number().over(w_running))
)

deduped = (
    scored
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

result = (
    deduped
    .select("id", "supplier_name", "country", "rating", "city_upper", "event_seq")
)
