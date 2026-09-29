from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("browser").orderBy("credit_limit", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("customer_id", "browser").orderBy("credit_limit", "id").rowsBetween(-5, Window.currentRow)
w3 = Window.partitionBy("browser").orderBy(F.col("credit_limit").desc())
w4 = Window.partitionBy("customer_id").orderBy("credit_limit", "id")
w_running = Window.partitionBy("browser").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("browser").orderBy(F.col("signup_date").desc(), "id")

with_dims = (
    df_sessions
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "left")
    .filter(F.col("p.country") == "TH")
    .select(
        "src.id",
        "src.customer_id",
        "src.browser",
        F.col("p.credit_limit"),
        F.col("p.signup_date"),
        F.col("p.age"),
    )
)

enriched = (
    with_dims
    .select(
        "id",
        "customer_id",
        "browser",
        "signup_date",
        F.avg("credit_limit").over(w_ordered).alias("credit_limit_avg_2"),
        F.avg("age").over(w2).alias("age_avg_5"),
        F.cume_dist().over(w3).alias("credit_limit_cume_dist"),
        F.ntile(3).over(w4).alias("credit_limit_bucket"),
        F.sum("age").over(w_running).alias("cumulative_age"),
    )
)

with_dims_sessions = (
    enriched
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "inner")
    .select("s.*", F.col("d.credit_limit"), F.col("d.name"), F.col("d.segment"))
)

deduped = (
    with_dims_sessions
    .withColumn("row_num", F.row_number().over(w6))
    .filter(F.col("row_num") == 1)
)

joined = (
    deduped
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id").eqNullSafe(F.col("c.id")), "left")
    .select(
        "e.id",
        "e.customer_id",
        "e.signup_date",
        "e.age_avg_5",
        "e.credit_limit_cume_dist",
        "e.credit_limit_bucket",
        "e.credit_limit",
        "e.row_num",
        F.col("c.country"),
        F.col("c.age"),
    )
)

result = (
    joined
    .select(
        "id",
        "customer_id",
        "age_avg_5",
        "credit_limit_cume_dist",
        "credit_limit_bucket",
        "row_num",
        "age",
    )
    .orderBy(F.col("row_num").asc_nulls_last())
)
