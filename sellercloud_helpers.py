from __future__ import annotations
from typing import Dict, List, Tuple
from collections import defaultdict
from utils import map_order_status
from seller_cloud_api import SellerCloudAPI


def get_orders_by_ids(
    sc_api: SellerCloudAPI, orders: List[dict]
) -> Tuple[Dict[str, List[dict]], List[dict], List[dict], List[dict]]:
    by_dropshipper: Dict[str, List[dict]] = defaultdict(list)
    cancelled: List[dict] = []
    on_hold: List[dict] = []
    problem: List[dict] = []

    for order in orders:
        sc_id = order.get("sellercloud_order_id")

        resp = sc_api.get_order(str(sc_id))
        if not resp.ok:
            print(f"Failed to fetch order {sc_id}: {resp.status_code} {resp.text}")
            continue
        data = resp.json()
        statuses = data.get("Statuses") or {}
        raw_status = statuses.get("OrderStatus")
        status = map_order_status(raw_status)
        tracking_number = None

        packages = data.get("OrderPackages") or []
        if isinstance(packages, list) and packages:
            pkg0 = packages[0] or {}
            tracking_number = pkg0.get("TrackingNumber")

        if not tracking_number:
            shipments = data.get("Shipments") or data.get("OrderShipments") or []
            if isinstance(shipments, list) and shipments:
                s0 = shipments[0] or {}
                tracking_number = s0.get("TrackingNumber")
                if not tracking_number:
                    spkgs = s0.get("Packages") or []
                    if isinstance(spkgs, list) and spkgs:
                        sp0 = spkgs[0] or {}
                        tracking_number = sp0.get("TrackingNumber")

        # Tracking date (keep your original preference order)
        ship_details = data.get("ShippingDetails") or {}
        tracking_date = ship_details.get("ShipDate")
        merged = {
            **order,
            "sc_status": status,
            "sc_status_raw": raw_status,
            "tracking_number": tracking_number,
            "tracking_date": tracking_date,
        }

        # Special rule: Cancelled + has tracking -> treat as processable
        if status == "Cancelled" and tracking_number:
            status_bucket = "Process"
        elif status == "ProblemOrder":
            status_bucket = "ProblemOrder"
        elif status == "OnHold":
            status_bucket = "OnHold"
        elif status == "Cancelled":
            status_bucket = "Cancelled"
        else:
            status_bucket = "Process"

        if status_bucket == "ProblemOrder":
            problem.append(merged)
            continue

        if status_bucket == "OnHold":
            on_hold.append(merged)
            continue

        if status_bucket == "Cancelled":
            cancelled.append(merged)
            continue

        # Only include orders WITH tracking in the final processing bucket. status_bucket == "Process"
        if tracking_number:
            code = order.get("dropshipper_code")
            by_dropshipper[code].append(merged)

    return dict(by_dropshipper), cancelled, on_hold, problem
