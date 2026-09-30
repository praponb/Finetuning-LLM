from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("is_active_flag").desc(), "id")
w2 = Window.partitionBy("city").orderBy("signup_date", "id")
w_ordered = Window.partitionBy("country").orderBy("is_active_flag", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered_2 = Window.partitionBy("segment").orderBy(F.col("is_active_flag").asc_nulls_last())
w5 = Window.partitionBy("country").orderBy(F.col("credit_limit").desc_nulls_first())
w6 = Window.orderBy("is_active_flag", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("city")

cleaned = (
    df_customers
    .filter((~F.col("segment").eqNullSafe(F.lit("SMB"))) & (F.col("signup_date") > F.lit("2023-07-08").cast("date")))
    .select("*", F.col("is_active").cast("int").alias("is_active_flag"))
)

ranked = (
    cleaned
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

ranked_customers = (
    ranked
    .withColumn("rank_in_group_2", F.row_number().over(w2))
    .filter(F.col("rank_in_group_2") == 1)
)

scored = (
    ranked_customers
    .filter(F.col("credit_limit") < 8100)
    .select(
        "*",
        F.first("is_active_flag", ignorenulls=True).over(w_ordered).alias("first_known_is_active_flag"),
        F.rank().over(w_ordered_2).alias("is_active_flag_rank"),
        F.percent_rank().over(w5).alias("credit_limit_percent_rank"),
        F.sum("is_active_flag").over(w6).alias("cumulative_is_active_flag"),
        F.round((F.col("credit_limit") / F.sum("credit_limit").over(w7)), 4).alias("credit_limit_share"),
    )
)

kept = (
    scored
    .alias("s")
    .join(df_accounts.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.opened_date") <= "2023-12-01"), "left_semi")
)

result = (
    kept
    .select(
        "id",
        "name",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        "is_active_flag",
        "rank_in_group",
        "cumulative_is_active_flag",
    )
    .orderBy(F.col("credit_limit").desc_nulls_last())
)
