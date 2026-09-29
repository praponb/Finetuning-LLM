from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy("launch_date_period", "id")
w2 = Window.partitionBy("supplier_id").orderBy("launch_date_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("supplier_id").orderBy("cost", "id").rowsBetween(-5, -1)
w4 = Window.partitionBy("country").orderBy(F.col("rating").desc_nulls_last())
w5 = Window.orderBy(F.col("is_new_session").desc_nulls_first())
w6 = Window.partitionBy("supplier_id").orderBy("cost", "id")
w7 = Window.partitionBy("country")

valid = (
    df_products
    .filter(F.col("brand").isNotNull() & F.col("cost").between(260, 410))
    .select(
        "id",
        "product_name",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        F.upper("category").alias("category_upper"),
        F.date_trunc("month", "launch_date").alias("launch_date_period"),
    )
)

gaps = (
    valid
    .withColumn("is_new_session", F.when(F.lag("launch_date_period").over(w).isNull() | (F.col("launch_date_period") > F.lag("launch_date_period").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessionized = gaps.withColumn("island_id", F.sum("is_new_session").over(w2))

enriched = (
    sessionized
    .alias("src")
    .join(df_suppliers.alias("p"), F.col("src.supplier_id") == F.col("p.id"), "left")
    .select(
        "src.id",
        "src.product_name",
        "src.price",
        "src.cost",
        "src.launch_date",
        "src.supplier_id",
        "src.category_upper",
        "src.launch_date_period",
        "src.is_new_session",
        F.col("p.supplier_name"),
        F.col("p.rating"),
        F.col("p.country"),
    )
)

enriched_products = (
    enriched
    .withColumn("rating_avg_5_prior", F.avg("rating").over(w3))
    .withColumn("rating_dense_rank", F.dense_rank().over(w4))
    .withColumn("is_new_session_rank", F.rank().over(w5))
    .withColumn("next_rating", F.lead("rating", 1).over(w6))
    .withColumn("corr_cost_price", F.corr("cost", "price").over(w7))
)

with_dims = (
    enriched_products
    .alias("a")
    .join(df_suppliers.alias("b"), F.col("a.supplier_id").eqNullSafe(F.col("b.id")), "inner")
    .filter(F.col("b.rating").between(1.8, 2.8))
    .select("a.*", F.col("b.city"), F.col("b.rating").alias("supplier_rating"))
)

result = (
    with_dims
    .groupBy("country", F.date_trunc("YEAR", "launch_date_period").alias("year_start"))
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("next_rating"), 2).alias("avg_next_rating"),
        F.count(F.when(F.col("city") == "New York", 1)).alias("new_york_rows"),
    )
    .orderBy(F.col("avg_next_rating").desc())
)
