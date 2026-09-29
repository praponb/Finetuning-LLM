from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "segment").orderBy(F.col("signup_date").desc_nulls_first(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.discount").desc(), F.col("c.id").desc())
w_grp = Window.orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("credit_limit_filled")

filtered = (
    df_customers
    .filter((F.col("credit_limit") >= 4700) & F.col("segment").isNotNull())
    .select(
        "id",
        "email",
        "country",
        "segment",
        "signup_date",
        "age",
        F.coalesce("credit_limit", F.lit(0)).alias("credit_limit_filled"),
    )
)

deduped = (
    filtered
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

latest_match = (
    deduped
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.email",
        "s.signup_date",
        "s.credit_limit_filled",
        F.col("c.amount"),
        F.col("c.order_date").alias("order_order_date"),
    )
)

metrics = (
    latest_match
    .filter(F.col("amount").between(550, 630))
    .withColumn("last_known_amount", F.last("amount", ignorenulls=True).over(w_grp))
    .withColumn("credit_limit_filled_dense_rank", F.dense_rank().over(w4))
    .withColumn("cumulative_amount", F.sum("amount").over(w_grp))
)

result = (
    metrics
    .select("id", "amount", "credit_limit_filled", "last_known_amount")
    .unpivot(
        ids=["id"],
        values=["amount", "credit_limit_filled", "last_known_amount"],
        variableColumnName="kpi",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
