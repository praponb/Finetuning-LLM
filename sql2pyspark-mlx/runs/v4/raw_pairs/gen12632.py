from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("rating").desc(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.stock_qty").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("city").orderBy(F.col("stock_qty").desc(), "id")

deduped = (
    df_suppliers
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

latest_match = (
    deduped
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.stock_qty"))
)

latest = (
    latest_match
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
)

kept = (
    latest
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.supplier_id") == 4), "left_anti")
)

result = (
    kept
    .groupBy("country")
    .pivot("city", ["Singapore", "Berlin"])
    .agg(F.max("rating").alias("amount"), F.count(F.lit(1)).alias("n"))
    .select(
        "country",
        F.col("Singapore_amount").alias("singapore_amount"),
        F.col("Singapore_n").alias("singapore_n"),
        F.col("Berlin_amount").alias("berlin_amount"),
        F.col("Berlin_n").alias("berlin_n"),
    )
)
