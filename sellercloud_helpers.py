from typing import Dict, Iterable
from utils import map_order_status

def get_orders_by_ids(sc_api, sellercloud_order_ids: Iterable[str]) -> Dict[str, dict]:
    """
    Call SellerCloud API for each order_id.
    Returns: {order_id: {"order_status": ..., "tracking_number": ..., "tracking_date": ...}}
    """
    results: Dict[str, dict] = {}

    for order_id in sellercloud_order_ids:
        resp = sc_api.get_order(order_id)
        if not resp.ok:
            # You can log and continue or raise — choose what fits ops best
            print(f"Failed to fetch order {order_id}: {resp.status_code} {resp.text}")
            continue

        order = resp.json()
        order_id = (
        order.get("OrderID")
        or order.get("Id")
        or order.get("ID") #Need to check exact name
        )
        
        # Status code path can vary
        statuses = order.get("Statuses") or {}
        status_code = (
            statuses.get("OrderStatus")
            or statuses.get("StatusCode")
            or statuses.get("Code")
            or None
        )
        
        # Tracking number: prefer first package if present
        packages = order.get("OrderPackages") or order.get("Packages") or []
        tracking_number = None
        if isinstance(packages, list) and packages:
            # Try common variants
            pkg0 = packages[0] or {}
            tracking_number = (
                pkg0.get("TrackingNumber")
                or pkg0.get("trackingNumber")
                or pkg0.get("Tracking")  # fallback if structure differs
            )
            
        # Tracking date: prefer ShippingDetails.ShipDate; fallback to another plausible field
        ship_details = order.get("ShippingDetails") or {}
        tracking_date = (
            ship_details.get("ShipDate")
            or order.get("ShipDate")
            or order.get("OrderDate")
            or order.get("DateCreated")
        )
        
        if order_id is None:
        # If we truly can't identify the order, log and skip
            print(f"GET_ORDER returned payload without usable OrderID: {order}")
            continue

        # Adjust keys to your exact API response
        results[order_id] = {
            "order_status":   map_order_status(status_code),
            "tracking_number": tracking_number,
            "tracking_date":   tracking_date,
        }

    return results