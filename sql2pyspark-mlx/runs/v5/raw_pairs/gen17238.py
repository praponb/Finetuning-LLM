from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("cost").desc_nulls_first(), "id")
w2 = Window.orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.orderBy("shipped_date", "id")

base = (
    df_shipments
    .filter(F.col("carrier").isNotNull() & (F.col("delivered_date") > F.lit("2023-04-25").cast("date")))
    .select(
        "id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        F.col("cost").cast("int").alias("cost_int"),
        F.length("carrier").alias("carrier_length"),
        F.coalesce("carrier", F.lit("OTHER")).alias("resolved_carrier"),
    )
)

base_shipments = (
    base
    .filter(F.col("carrier").isNotNull())
    .select(
        "id",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        "cost_int",
        "carrier_length",
        "resolved_carrier",
        F.add_months("delivered_date", 1).alias("delivered_date_plus_months"),
        F.last_day("shipped_date").alias("shipped_date_month_end"),
    )
)

deduped = (
    base_shipments
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "shipped_date",
        "cost",
        "carrier_length",
        "delivered_date_plus_months",
    )
)

valid = (
    deduped
    .filter(F.col("shipped_date") > F.lit("2023-04-15").cast("date"))
    .select(
        "id",
        "shipped_date",
        "cost",
        "carrier_length",
        "delivered_date_plus_months",
        F.round((F.col("cost") * 1.07), 2).alias("cost_scaled"),
    )
)

scored = (
    valid
    .select(
        "*",
        F.last("cost", ignorenulls=True).over(w2).alias("last_known_cost"),
        F.row_number().over(w_grp).alias("seq_no"),
    )
)

result = (
    scored
    .select("id", "shipped_date", "cost", "last_known_cost", "seq_no")
    .orderBy(F.col("last_known_cost").asc_nulls_first())
)
