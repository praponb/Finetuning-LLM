from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("country").orderBy("rating", "id")
w_grp = Window.orderBy("next_rating", "id").rowsBetween(-6, Window.currentRow)
w3 = Window.partitionBy("city").orderBy(F.col("next_rating").desc(), "id")
w4 = Window.orderBy(F.col("next_rating").asc_nulls_first())
w5 = Window.partitionBy("city")
w_part = Window.partitionBy("city").orderBy("seq_no", "id")
w7 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.launch_date").desc(), F.col("c.id").desc())

scored = (
    df_suppliers
    .select(
        "id",
        "supplier_name",
        "city",
        F.lead("rating").over(w_ordered).alias("next_rating"),
        F.row_number().over(w_ordered).alias("seq_no"),
    )
)

metrics = (
    scored
    .filter(F.col("next_rating") > 4.0)
    .select(
        "*",
        F.avg("next_rating").over(w_grp).alias("next_rating_avg_6"),
        F.ntile(5).over(w3).alias("next_rating_bucket"),
        F.percent_rank().over(w4).alias("next_rating_percent_rank"),
        F.sum("next_rating").over(w5).alias("city_sum_next_rating"),
        F.lead("seq_no", 1).over(w_part).alias("next_seq_no"),
    )
)

base = (
    metrics
    .filter(F.col("city").isNotNull() & F.col("city_sum_next_rating").between(10, 19))
    .select(
        "*",
        (F.col("next_rating") / F.nullif("next_rating_percent_rank", F.lit(0))).alias("next_rating_per_next_rating_percent_rank"),
        F.coalesce("city", F.lit("OTHER")).alias("resolved_city"),
    )
)

with_last = (
    base
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w7))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.launch_date").alias("product_launch_date"),
        F.col("c.price").alias("product_price"),
    )
)

result = (
    with_last
    .groupBy(F.date_trunc("WEEK", "product_launch_date").alias("week_start"))
    .agg(
        F.percentile_approx("next_rating", [0.25, 0.5, 0.75]).alias("next_rating_quartiles"),
        F.count(F.when(F.col("city") == "Singapore", 1)).alias("singapore_count"),
    )
)
