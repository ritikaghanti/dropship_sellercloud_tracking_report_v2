from __future__ import annotations
from typing import Dict, List, Tuple
from collections import defaultdict
from utils import map_order_status

def get_orders_by_ids(sc_api, orders: List[dict]) -> Tuple[Dict[str, List[dict]], List[dict], List[dict], List[dict]]:
    """
    Input:
      orders: flat list from DropshipDb.get_untracked_orders()

    Output:
      by_dropshipper: { "AAG": [ merged_order, ... ], ... }  # only NOT Cancelled/OnHold/ProblemOrder
      cancelled:      [ merged_order, ... ]
      on_hold:        [ merged_order, ... ]
      problem:        [ merged_order, ... ]

    merged_order = original DB order + SC fields:
      sc_status (mapped), sc_status_raw (raw), tracking_number, tracking_date
    """
    by_dropshipper: Dict[str, List[dict]] = defaultdict(list)
    cancelled: List[dict] = []
    on_hold: List[dict] = []
    problem: List[dict] = []

    for order in orders:
        sc_id = order.get("sellercloud_order_id")
        if not sc_id:
            # If DB row doesn't include SC ID, skip or resolve by PO#
            continue

        resp = sc_api.get_order(str(sc_id))
        if not resp.ok:
            print(f"Failed to fetch order {sc_id}: {resp.status_code} {resp.text}")
            continue

        data = resp.json()

        # ---- RAW STATUS (from SellerCloud payload, not the DB order) ----
        statuses = data.get("Statuses") or {}
        raw_status = (
            statuses.get("OrderStatus")
            or statuses.get("Status")
            or data.get("Status")
            or data.get("OrderStatus")
        )
        sc_order_id = data.get("OrderID") or data.get("Id") or data.get("ID")
        print(f"[SC STATUS] OrderID={sc_order_id} raw={raw_status!r} type={type(raw_status).__name__}")

        # Map raw status to our normalized buckets
        status = map_order_status(raw_status)

        # ---- Tracking extraction with a few fallbacks ----
        tracking_number = None

        # Try top-level packages
        packages = data.get("OrderPackages") or data.get("Packages") or []
        if isinstance(packages, list) and packages:
            pkg0 = packages[0] or {}
            tracking_number = (
                pkg0.get("TrackingNumber")
                or pkg0.get("trackingNumber")
                or pkg0.get("Tracking")
                or pkg0.get("tracking")
            )

        # Try shipments if packages didn't work
        if not tracking_number:
            shipments = data.get("Shipments") or data.get("OrderShipments") or []
            if isinstance(shipments, list) and shipments:
                s0 = shipments[0] or {}
                tracking_number = (
                    s0.get("TrackingNumber")
                    or s0.get("trackingNumber")
                    or s0.get("Tracking")
                )
                if not tracking_number:
                    spkgs = s0.get("Packages") or []
                    if isinstance(spkgs, list) and spkgs:
                        sp0 = spkgs[0] or {}
                        tracking_number = (
                            sp0.get("TrackingNumber")
                            or sp0.get("trackingNumber")
                            or sp0.get("Tracking")
                        )

        # Tracking date (keep your original preference order)
        ship_details = data.get("ShippingDetails") or {}
        tracking_date = (
            ship_details.get("ShipDate")
            or data.get("ShipDate")
            or data.get("OrderDate")
            or data.get("DateCreated")
        )

        merged = {
            **order,  # DB fields
            "sc_status": status,
            "sc_status_raw": raw_status,
            "tracking_number": tracking_number,
            "tracking_date": tracking_date,
        }

        if status == "Cancelled":
            cancelled.append(merged)
        elif status == "OnHold":
            on_hold.append(merged)
        elif status == "ProblemOrder":
            problem.append(merged)
        else:
            code = order.get("dropshipper_code") or "UNKNOWN"
            by_dropshipper[code].append(merged)

    return dict(by_dropshipper), cancelled, on_hold, problem