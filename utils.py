def map_order_status(code: int | None) -> str | None:
    mapping = {
        -1: "Cancelled",
        200: "OnHold",
        100: "ProblemOrder",
    }
    return mapping.get(code)