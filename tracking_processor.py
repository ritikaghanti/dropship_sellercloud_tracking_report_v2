from typing import Dict, List, Tuple
from dropship_db import DropshipDb
import io
import csv
# import os

from datetime import datetime
from kramer_functions import FTPFileManager

shipping_methods: Dict[str, Dict[str, str]] = {
    "UPS Ground":      {"name": "UPS",   "code": "UPS"},
    "FEDEX Ground HD": {"name": "FedEx", "code": "FEDHD"},
    }

class TrackingReportProcessor:
    """
    Orchestrates:
    - status handling (cancel/backorder/problem)
    - enerating tracking rows in memory (no RowCreator)
    - uploading CSV via FTP/SFTP from memory (no FileHandler)
    - saving tracking back to DB
    """
    
    def __init__(self, d_db: DropshipDb, sc_api):
        self.d_db = d_db
        self.sc_api = sc_api
        self.errors = {"failed_to_process": []}
        self.ftp = FTPFileManager()
        
        # ---------------- internal helpers ----------------
        
    def _handle_status_side_effects(self, order: dict, status: str | None) -> None:
        po_num = order["purchase_order_number"]
        if status == "Cancelled":
            self.d_db.turning_on_is_cancelled_status(po_num)
        elif status == "OnHold":
            self.d_db.turning_on_is_backorder_status(po_num)
        elif status == "ProblemOrder":
            pass
        
    def _build_rows_for_order(self, order: dict, sc_info: dict) -> List[dict]:
        """
        Build flat rows (one per item) for CSV output.
        Columns chosen to mirror typical tracking exports.
        Adjust/add columns if your recipient expects a different schema.
        """
        tracking_number = sc_info.get("tracking_number")
        tracking_date = sc_info.get("tracking_date")
        if not tracking_number:
            self.errors.setdefault("missing_tracking", []).append(
            order["purchase_order_number"])
            print(f"Order {order['purchase_order_number']} has no tracking number.")
            return []

        order["tracking_number"] = tracking_number
        order["tracking_date"] = tracking_date

        # return self.row_creator.create_tracking_objects(
        #     order["purchase_order_number"],
        #     order["ship_method"],
        #     tracking_number,
        #     order["items"],
        #     order["tracking_date"],
        # )
        
        # Map carrier and code; fall back gracefully if ship_method not mapped
        raw_ship_method = order.get("ship_method")
        mapping = shipping_methods.get(raw_ship_method, {})
        carrier_name = mapping.get("name", "")              # default empty if not mapped
        ship_method_code = mapping.get("code", "")          # default empty if not mapped

        # Old RowCreator always set "Ground" as display method
        display_ship_method = "Ground"

        
        rows: List[dict] = []
        for it in order.get("items", []):
            rows.append({
                "purchase_order_number": order["purchase_order_number"],
                "sellercloud_order_id": order["sellercloud_order_id"],
                "carrier_name":         carrier_name,
                "ship_method":          display_ship_method,
                "ship_method_code":     ship_method_code,
                "tracking_number": tracking_number,
                "tracking_date": tracking_date,
                "sku": it.get("sku"),
                "quantity": it.get("quantity"),
            })
        return rows
    
    def _rows_to_csv_bytes(self, rows: List[dict]) -> bytes:
        """
        Serialize rows to CSV in memory and return bytes for upload.
        """
        if not rows:
            return b""

        # stable column order
        columns = [
            "purchase_order_number",
            "sellercloud_order_id",
            "carrier_name",
            "ship_method",
            "ship_method_code",
            "tracking_number",
            "tracking_date",
            "sku",
            "quantity",
        ]

        buf = io.StringIO(newline="")
        writer = csv.DictWriter(buf, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
        return buf.getvalue().encode("utf-8")
    
    
    def _remote_csv_path(self, ftp_folder: str, dropshipper_code: str) -> str:
        """
        Decide remote path/name. Tweak to match partner expectations.
        """
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        fname = f"tracking_{dropshipper_code}_{ts}.csv"
        # ensure single leading slash and no double slashes
        ftp_folder = ftp_folder.strip("/ ")
        return f"/{ftp_folder}/{fname}" if ftp_folder else f"/{fname}"

    def _process_one_dropshipper(
        self, dropshipper_code: str, orders: List[dict], sellercloud_orders: Dict[str, dict]
    ) -> Tuple[str | None, List[str]]:
        """
        Returns (file_path or None, processed_sellercloud_ids)
        """
        all_rows: List[dict] = []
        processed_ids: List[str] = []

        for order in orders:
            sc_id = order["sellercloud_order_id"]
            sc_info = sellercloud_orders.get(sc_id)
            if not sc_info:
                print(f"Order {sc_id} not found in SellerCloud lookup; skipping.")
                continue

            self._handle_status_side_effects(order, sc_info.get("order_status"))

            rows = self._build_rows_for_order(order, sc_info)
            if rows:
                all_rows.extend(rows)
                processed_ids.append(str(sc_id))

        if not all_rows:
            print(f"No orders from {dropshipper_code} were tracked.")
            return None, processed_ids

        ftp_folder = orders[0]["ftp_folder_name"]  # consistent folder for that dropshipper
        remote_path = self._remote_csv_path(ftp_folder, dropshipper_code)
        csv_bytes = self._rows_to_csv_bytes(all_rows)
        self.ftp.upload_bytes(csv_bytes, remote_path)
        return remote_path, processed_ids

    def run(self, untracked_orders: Dict[str, List[dict]], sellercloud_orders: Dict[str, dict]) -> dict:
        remote_files: List[str] = []
        orders_processed: List[str] = []

        try:
        # Process and upload per dropshipper
            for dropshipper_code, orders in untracked_orders.items():
                remote_path, processed_ids = self._process_one_dropshipper(
                    dropshipper_code, orders, sellercloud_orders
                )
                if remote_path:
                    remote_files.append(remote_path)
                orders_processed.extend(processed_ids)

        # Persist tracking info back to DB (only if there are any)
            if untracked_orders:
                self.d_db.save_tracking_data(untracked_orders)

            return {
                "files_uploaded": remote_files,
                "orders_processed": orders_processed,
                "errors": self.errors,
                "missing_tracking": self.errors.get("missing_tracking", []),
                # "orders_processed_by_email": {...}  # add if you group for notifications
            }
        finally:
            # Always close the FTP/SFTP connection
            try:
                self.ftp.close()
            except Exception:
                pass