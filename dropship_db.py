from __future__ import annotations
from typing import List, Dict
import pyodbc
from kramer_functions import AzureSecrets

def _parse_sku(s: str) -> Dict[str, object]:
    """
    Accept SKU formats like: A0012/B0012 OR A0012 OR A0012/B00012/C0012/D0012
    Store both raw and split components.
    """
    s = (s or "").strip()
    parts = [p.strip() for p in s.split("/") if p.strip()]
    return {"sku_raw": s, "sku_parts": parts}


class DropshipDb:
    def __init__(self):
        self.secrets = AzureSecrets()
        self.connection_string = self.secrets.get_connection_string("DropshipSellerCloudTest")
        self.conn = pyodbc.connect(self.connection_string)

    def close(self):
        if self.conn:
            self.conn.close()

    def get_untracked_orders(self) -> List[dict]:
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                """
                SELECT
                    d.sellercloud_customer_id,
                    d.code AS dropshipper_code,
                    po.id,
                    po.purchase_order_number,
                    po.sellercloud_order_id,
                    po.date_added,
                    po.customer_first_name,
                    po.customer_last_name,
                    po.phone,
                    po.address,
                    po.city,
                    s.name AS state,
                    po.zip,
                    c.two_letter_code AS country,
                    po.dropshipper_id,
                    te.is_exempt,
                    d.kramer_shipping_account AS ships_with_kramer,
                    d.ship_method,
                    poi.sku,
                    poi.quantity,
                    poi.price,
                    COALESCE(vpa_sku.shipping_cost, vpa_alias.shipping_cost) AS shipping_cost,
                    d.email_notifications,
                    d.invoice_email
                FROM PurchaseOrders po
                JOIN Dropshippers d ON po.dropshipper_id = d.id
                JOIN States s ON po.state = s.id
                JOIN Countries c ON po.country = c.id
                JOIN TaxExempt te ON po.dropshipper_id = te.dropshipper_id AND po.state = te.state_id
                JOIN PurchaseOrderItems poi ON po.id = poi.purchase_order_id
                LEFT JOIN PurchaseOrderErrors poe
                    ON poe.purchase_order_id = po.id AND poe.resolved = 0
                OUTER APPLY (
                    SELECT TOP 1 shipping_cost FROM vProductAndAliases WHERE sku = poi.sku
                ) vpa_sku
                OUTER APPLY (
                    SELECT TOP 1 shipping_cost FROM vProductAndAliases
                    WHERE alias = poi.sku
                    AND NOT EXISTS (SELECT 1 FROM vProductAndAliases WHERE sku = poi.sku)
                ) vpa_alias
                WHERE po.is_cancelled = 0
                    AND po.in_sellercloud = 1
                    AND po.tracking_number IS NULL
                    AND d.code != 'ABS'
                    AND poe.purchase_order_id IS NULL
                """
            )

            cols = [c[0] for c in cursor.description]
            by_id: Dict[int, dict] = {}

            for row in cursor.fetchall():
                r = dict(zip(cols, row))
                po_id = r["id"]

                if po_id not in by_id:
                    by_id[po_id] = {
                        "id": po_id,
                        "sellercloud_customer_id": r["sellercloud_customer_id"],
                        "dropshipper_code": r["dropshipper_code"],
                        "purchase_order_number": r["purchase_order_number"],
                        "sellercloud_order_id": r["sellercloud_order_id"],   # ✅ now present
                        "date_added": r["date_added"],
                        "customer_first_name": r["customer_first_name"],
                        "customer_last_name": r["customer_last_name"],
                        "phone": r["phone"],
                        "address": r["address"],
                        "city": r["city"],
                        "state": r["state"],
                        "zip": r["zip"],
                        "country": r["country"],
                        "dropshipper_id": r["dropshipper_id"],
                        "is_exempt": r["is_exempt"],
                        "ships_with_kramer": r["ships_with_kramer"],
                        "ship_method": r["ship_method"],
                        "email_notifications": r["email_notifications"],
                        "invoice_email": r["invoice_email"],
                        "items": [],
                    }

                sku_bits = _parse_sku(r["sku"])
                by_id[po_id]["items"].append({
                    "sku": sku_bits["sku_raw"],
                    "sku_parts": sku_bits["sku_parts"],
                    "quantity": r["quantity"],
                    "price": r["price"],
                    "shipping_cost": r["shipping_cost"],
                })

            return list(by_id.values())

        except Exception as e:
            print(f"Error while getting untracked orders: {e}")
            raise
        
    def save_tracking_data(self, by_dropshipper: Dict[str, List[dict]]) -> int:
        """
        Persist tracking info back to the DB.
        by_dropshipper: { "AAG": [order_dict, ...], ... }
        Each order is expected to contain:
        - purchase_order_number (str)
        - tracking_number (str)
        - tracking_date (str/datetime or None)

        Returns: number of rows updated.
        """
        cursor = self.conn.cursor()
        updated = 0

        for orders in by_dropshipper.values():
            for order in orders:
                tn = (order or {}).get("tracking_number")
                if not tn:
                    continue  # skip orders without tracking

                td = (order or {}).get("tracking_date")
                po = (order or {}).get("purchase_order_number")
                if not po:
                    continue

                # If your schema uses a different column for date (e.g., ShipDate),
                # change 'tracking_date' below accordingly.
                cursor.execute(
                    """
                    UPDATE PurchaseOrders
                    SET tracking_number = ?,
                        tracking_date   = ?
                    WHERE purchase_order_number = ?
                    AND (tracking_number IS NULL OR tracking_number = '')
                    """,
                    (tn, td, po),
                )
                updated += cursor.rowcount

        self.conn.commit()
        return updated