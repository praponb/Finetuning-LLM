from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("rating").desc(), "id")
w_ordered = Window.partitionBy("country").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("running_min_rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("country").orderBy("running_count_rating", "id")

ranked = (
    df_suppliers
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "supplier_name", "country", "rating")
)

metrics = (
    ranked
    .select(
        "id",
        "supplier_name",
        "country",
        F.min("rating").over(w_ordered).alias("running_min_rating"),
        F.sum("rating").over(w_ordered).alias("running_sum_rating"),
        F.count("rating").over(w_ordered).alias("running_count_rating"),
    )
)

enriched = (
    metrics
    .select(
        "*",
        F.sum("running_min_rating").over(w3).alias("cumulative_running_min_rating"),
        F.row_number().over(w4).alias("seq_no"),
    )
)

eligible = (
    enriched
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.length(F.col("ch.product_name")) >= 7), "left_anti")
)

cleaned = (
    eligible
    .filter(F.col("country").isin(["TH", "DE"]))
    .select("*", F.col("seq_no").cast("string").alias("seq_no_str"))
)

result = (
    cleaned
    .select(
        "id",
        "country",
        "running_min_rating",
        "running_sum_rating",
        "running_count_rating",
        "seq_no",
        "seq_no_str",
        F.when(F.col("country").rlike("^(TH|UK)$"), "OTHER_ZONE").otherwise("CORE").alias("country_zone"),
    )
)
