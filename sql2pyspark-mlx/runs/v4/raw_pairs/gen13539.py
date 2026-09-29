from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "city").orderBy(F.col("rating").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.launch_date").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("category").orderBy(F.col("rating").desc(), "id")
w4 = Window.partitionBy("category").orderBy(F.col("rating").desc())
w5 = Window.orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("country").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

deduped = (
    df_suppliers
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

latest_match = (
    deduped
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.category"))
)

metrics = (
    latest_match
    .withColumn("rating_bucket", F.ntile(10).over(w3))
    .withColumn("rating_dense_rank", F.dense_rank().over(w4))
    .withColumn("cumulative_rating", F.sum("rating").over(w5))
    .withColumn("cumulative_rating_2", F.sum("rating").over(w6))
)

result = (
    metrics
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating_bucket",
        "cumulative_rating",
        F.when(F.col("country").rlike("^(JP|SG)$"), "EXTENDED").when(F.col("country").rlike("^(TH|US)$"), "OTHER_ZONE").otherwise("GROUP_B").alias("country_zone"),
    )
    .orderBy(F.col("rating_bucket").desc())
)
