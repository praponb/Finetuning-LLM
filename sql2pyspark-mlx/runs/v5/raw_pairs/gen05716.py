from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("units").desc(), "id")
w2 = Window.partitionBy("store_id").orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("store_id").orderBy("sale_date", "id")
amount_by_store_id = df_orders.groupBy("store_id").agg(F.sum("amount").alias("sum_order_amount"))

filtered = (
    df_store_sales
    .filter(F.col("sale_date") > F.lit("2023-04-13").cast("date"))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        F.weekofyear("sale_date").alias("sale_date_weekofyear"),
        F.add_months("sale_date", 3).alias("sale_date_plus_months"),
    )
)

with_windows = (
    filtered
    .select(
        "*",
        F.row_number().over(w).alias("event_seq"),
        F.sum("units").over(w2).alias("running_units"),
    )
)

valid = (
    with_windows
    .filter(F.col("units") != 36)
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "sale_date_weekofyear",
        "sale_date_plus_months",
        "event_seq",
        "running_units",
        F.round("event_seq", 0).alias("event_seq_rounded"),
    )
)

ranked = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

cleaned = (
    ranked
    .filter(F.month("sale_date").isin([3, 4, 5]))
    .select(
        "id",
        "store_id",
        "sale_date",
        "units",
        "sale_date_weekofyear",
        "event_seq",
        F.datediff(F.current_date(), "sale_date_plus_months").alias("days_since_sale_date_plus_months"),
        F.col("sale_date_weekofyear").cast("string").alias("sale_date_weekofyear_str"),
    )
)

with_dims = (
    cleaned
    .alias("s")
    .join(df_stores.alias("p"), F.col("s.store_id") == F.col("p.id"), "inner")
    .select("s.*", F.col("p.city"))
)

joined = (
    with_dims
    .alias("src")
    .join(df_stores.alias("d"), F.col("src.store_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.opened_date"), F.col("d.city").alias("store_city"))
)

result = (
    joined
    .join(amount_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "store_id",
        "sale_date",
        "days_since_sale_date_plus_months",
        "sale_date_weekofyear_str",
        F.col("sum_order_amount"),
    )
)
