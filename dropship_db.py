import pyodbc
from kramer_functions import AzureSecrets
from typing import Dict, List

class DropshipDb():
    def __init__(self):
        self.secrets = AzureSecrets()
        self.connection_string = self.secrets.get_connection_string(
            "DropshipSellerCloudTest"
        )
        self.conn = pyodbc.connect(self.connection_string)
        
    def close(self):
        if self.conn:
            self.conn.close()
            
    def get_untracked_orders(self):
        """Gets all the untracked orders from the Dropship database."""
        try:
            cursor = self.conn.cursor()

            # 1) Fetch all untracked purchase orders
            cursor.execute(
                """
                SELECT   
                    po.id                           AS po_id,     
                    po.purchase_order_number,
                    po.sellercloud_order_id,
                    d.code                          AS dropshipper_code,
                    d.ftp_folder_name,
                    d.kramer_shipping_account       AS ships_with_kramer,
                    d.ship_method
                FROM PurchaseOrders po
                JOIN Dropshippers d ON po.dropshipper_id = d.id
                WHERE po.is_cancelled = 0
                AND po.in_sellercloud = 1
                AND po.tracking_number IS NULL
                AND d.code != 'ABS'
                """
            )
            rows = cursor.fetchall()

            dropshippers_untracked_orders = {}
            sellercloud_order_ids = []
            orders_by_id = {}

            if not rows:
                return dropshippers_untracked_orders, sellercloud_order_ids

            # Map orders by id and prepare sellercloud_order_ids
            for row in rows:
                if row.sellercloud_order_id is not None:
                    sellercloud_order_ids.append(str(row.sellercloud_order_id))

                orders_by_id[row.po_id] = {
                    "purchase_order_number": row.purchase_order_number,
                    "sellercloud_order_id": row.sellercloud_order_id,
                    "ftp_folder_name": row.ftp_folder_name,
                    "ships_with_kramer": row.ships_with_kramer,
                    "ship_method": row.ship_method,
                    "dropshipper_code": row.dropshipper_code,
                    "items": []
                }

            # 2) Fetch all items for these purchase orders
            po_ids = list(orders_by_id.keys())
            for i in range(0, len(po_ids), 50):  # batching in case of large number
                batch_ids = po_ids[i:i + 50]
                placeholders = ",".join("?" for _ in batch_ids)
                cursor.execute(
                    f"""
                    SELECT purchase_order_id, sku, quantity
                    FROM PurchaseOrderItems
                    WHERE purchase_order_id IN ({placeholders})
                    """,
                    batch_ids
                )
                for item_row in cursor.fetchall():
                    orders_by_id[item_row.purchase_order_id]["items"].append({
                        "sku": item_row.sku,
                        "quantity": item_row.quantity
                    })

            # 3) Group orders by dropshipper code
            for order in orders_by_id.values():
                code = order.pop("dropshipper_code")
                dropshippers_untracked_orders.setdefault(code, []).append(order)

            return dropshippers_untracked_orders, sellercloud_order_ids

        except Exception as e:
            print(f"Error while getting untracked orders: {e}")
            raise
        # -------------------------------------------------------------
    # WRITE: Save tracking back to PurchaseOrders
    # -------------------------------------------------------------
    def save_tracking_data(self, untracked_orders: Dict[str, List[dict]]) -> None:
        """
        Persist tracking info for orders that now have tracking_number set.

        untracked_orders: { dropshipper_code: [ {purchase_order_number, tracking_number, tracking_date?}, ... ] }
        Only rows with tracking_number will be updated.
        """
        cursor = self.conn.cursor()
        updates = 0

        try:
            for orders in untracked_orders.values():
                for order in orders:
                    tn = order.get("tracking_number")
                    if not tn:
                        continue  # skip orders that still have no tracking

                    po_num = order["purchase_order_number"]
                    tdate = order.get("tracking_date")

                    # If your schema has tracking_date, keep it; otherwise remove that column.
                    cursor.execute(
                        """
                        UPDATE PurchaseOrders
                        SET tracking_number = ?,
                            tracking_date   = COALESCE(?, tracking_date)
                        WHERE purchase_order_number = ?
                        """,
                        (tn, tdate, po_num),
                    )
                    updates += cursor.rowcount

            if updates:
                self.conn.commit()
        except Exception:
            # On any failure, rollback partial updates
            self.conn.rollback()
            raise

    # -------------------------------------------------------------
    # WRITE: Helpers used by the processor for status side-effects
    # -------------------------------------------------------------
    def turning_on_is_cancelled_status(self, purchase_order_number: str) -> None:
        """Set is_cancelled=1 for the given PO number."""
        cursor = self.conn.cursor()
        try:
            cursor.execute(
                """
                UPDATE PurchaseOrders
                SET is_cancelled = 1
                WHERE purchase_order_number = ?
                """,
                (purchase_order_number,),
            )
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

    def turning_on_is_backorder_status(self, purchase_order_number: str) -> None:
        """Set a backorder/hold flag for the given PO number (adjust column name if different)."""
        cursor = self.conn.cursor()
        try:
            # If your schema uses a different column (e.g., is_backorder or on_hold), change it here.
            cursor.execute(
                """
                UPDATE PurchaseOrders
                SET is_backorder = 1
                WHERE purchase_order_number = ?
                """,
                (purchase_order_number,),
            )
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise
    