from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "country").orderBy(F.col("credit_limit").asc_nulls_first(), "id")

cleaned = (
    df_accounts
    .filter(F.col("opened_date") > F.lit("2023-06-22").cast("date"))
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        F.date_add("opened_date", 47).alias("opened_date_plus_47d"),
    )
)

with_dims = (
    cleaned
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "inner")
    .select("src.*", F.col("p.credit_limit"), F.col("p.country"))
)

deduped = (
    with_dims
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    deduped
    .groupBy("customer_id", "status")
    .pivot("country", ["UK", "JP", "DE"])
    .agg(F.avg("credit_limit"))
)
