from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("price", "id")
w2 = Window.orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("supplier_id").orderBy("cost", "id").rowsBetween(-3, -1)
w4 = Window.partitionBy("supplier_id").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("supplier_id")
w6 = Window.partitionBy("country").orderBy("running_cost", "id")

ranked = (
    df_products
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "price", "cost", "supplier_id")
)

metrics = (
    ranked
    .withColumn("cumulative_cost", F.sum("cost").over(w2))
    .withColumn("price_avg_3_prior", F.avg("price").over(w_running))
)

scored = (
    metrics
    .filter(F.col("price") < 780)
    .select(
        "id",
        "cost",
        "supplier_id",
        "price_avg_3_prior",
        F.sum("cost").over(w4).alias("running_cost"),
        F.round((F.col("price_avg_3_prior") / F.sum("price_avg_3_prior").over(w5)), 4).alias("price_avg_3_prior_share"),
    )
)

with_dims = (
    scored
    .alias("a")
    .join(df_suppliers.alias("b"), F.col("a.supplier_id") == F.col("b.id"), "inner")
    .select(
        "a.id",
        "a.cost",
        "a.supplier_id",
        "a.running_cost",
        F.col("b.city"),
        F.col("b.rating"),
        F.col("b.country"),
    )
)

result = (
    with_dims
    .select(
        "id",
        "cost",
        "supplier_id",
        "country",
        F.lag("rating").over(w6).alias("prev_rating"),
        F.lead("rating").over(w6).alias("next_rating"),
        F.lead("running_cost").over(w6).alias("next_running_cost"),
        F.row_number().over(w6).alias("event_seq"),
    )
)
