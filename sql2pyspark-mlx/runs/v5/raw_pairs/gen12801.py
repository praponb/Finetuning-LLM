from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(-6, -1)
w2 = Window.partitionBy("city").orderBy("rating", "id")
w3 = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("city").orderBy("seq_no", "id")
w5 = Window.partitionBy("city").orderBy("seq_no", "id").rowsBetween(-2, Window.currentRow)
w6 = Window.partitionBy("city").orderBy(F.col("rating").desc(), "id")
w7 = Window.partitionBy("city").orderBy("rating_avg_6_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("city").orderBy("rating_avg_6_prior_avg_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy("seq_no", "id")

base = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "supplier_name",
        "city",
        "rating",
        F.concat("country", F.lit(", "), "supplier_name").alias("country_supplier_name"),
        F.coalesce("city", F.lit("NONE")).alias("resolved_city"),
    )
)

scored = (
    base
    .filter(F.col("city").isin(["Berlin", "London", "New York"]))
    .select(
        "id",
        "supplier_name",
        "city",
        "rating",
        "country_supplier_name",
        F.avg("rating").over(w).alias("rating_avg_6_prior"),
        F.row_number().over(w2).alias("seq_no"),
        F.first("city", ignorenulls=True).over(w3).alias("first_known_city"),
    )
)

not_flagged = (
    scored
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.launch_date") >= "2023-05-01"), "left_anti")
)

scored_suppliers = (
    not_flagged
    .withColumn("prev_rating", F.lag("rating", 2).over(w4))
    .withColumn("seq_no_bucket", F.ntile(10).over(w4))
    .withColumn("rating_avg_6_prior_avg_2", F.avg("rating_avg_6_prior").over(w5))
    .withColumn("rating_bucket", F.ntile(3).over(w6))
    .withColumn("running_rating_avg_6_prior", F.sum("rating_avg_6_prior").over(w7))
)

enriched = (
    scored_suppliers
    .select(
        "id",
        "supplier_name",
        "rating",
        "country_supplier_name",
        "seq_no_bucket",
        "rating_avg_6_prior_avg_2",
        "rating_bucket",
        "running_rating_avg_6_prior",
        F.sum("rating_avg_6_prior_avg_2").over(w_grp).alias("cumulative_rating_avg_6_prior_avg_2"),
        F.row_number().over(w9).alias("event_seq"),
    )
)

result = (
    enriched
    .select(
        "id",
        "supplier_name",
        "country_supplier_name",
        "rating_avg_6_prior_avg_2",
        "event_seq",
    )
    .orderBy(F.col("event_seq").desc())
)
