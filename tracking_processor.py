from typing import Dict, List, Tuple, Optional
from dropship_db import DropshipDb
import io
import csv
from datetime import datetime
from kramer_functions import FTPFileManager

shipping_methods: Dict[str, Dict[str, str]] = {
    "UPS Ground":      {"name": "UPS",   "code": "UPS"},
    "FEDEX Ground HD": {"name": "FedEx", "code": "FEDHD"},
}

class TrackingReportProcessor:
    """
    Orchestrates:
    - generating tracking rows in memory (no RowCreator)
    - saving tracking back to DB
    """
    def __init__(self, d_db: DropshipDb, sc_api):
        self.d_db = d_db
        self.sc_api = sc_api
        self.errors = {"failed_to_process": []}
        self.ftp: Optional[FTPFileManager] = None  # lazy-init
        self.orders_processed_by_email = {}

    # -------------- FTP helpers --------------
    def _ensure_ftp(self) -> None:
        if self.ftp is None:
            self.ftp = FTPFileManager()

    def _safe_close_ftp(self) -> None:
        if self.ftp is None:
            return
        try:
            self.ftp.close()
        except EOFError: # Server already closed connection; safe to ignore on shutdown
            pass
        except Exception: # Swallow close-time errors
            pass
        finally:
            self.ftp = None
            
    def _build_rows_for_order(self, order: dict) -> List[dict]:
        tracking_number = order.get("tracking_number")
        tracking_date = order.get("tracking_date")
        if not tracking_number:
            self.errors.setdefault("missing_tracking", []).append(order["purchase_order_number"])
            print(f"Order {order['purchase_order_number']} has no tracking number.")
            return []

        # Map carrier & method code
        raw_ship_method = order.get("ship_method")
        mapping = shipping_methods.get(raw_ship_method, {})
        carrier_name = mapping.get("name", "")
        ship_method_code = mapping.get("code", "")
        display_ship_method = "Ground"  # legacy display

        rows: List[dict] = []
        for it in order.get("items", []):
            rows.append({
                "purchase_order_number": order["purchase_order_number"],
                "sellercloud_order_id": order.get("sellercloud_order_id"),
                "carrier_name":         carrier_name,
                "ship_method":          display_ship_method,
                "ship_method_code":     ship_method_code,
                "tracking_number":      tracking_number,
                "tracking_date":        tracking_date,
                "sku":                  it.get("sku"),
                "quantity":             it.get("quantity"),
            })
        return rows

    def _rows_to_csv_bytes(self, rows: List[dict]) -> bytes:
        if not rows:
            return b""
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
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        fname = f"tracking_{dropshipper_code}_{ts}.csv"
        ftp_folder = (ftp_folder or "").strip("/ ")
        return f"/{ftp_folder}/{fname}" if ftp_folder else f"/{fname}"

    def _process_one_dropshipper(self, dropshipper_code: str, orders: List[dict]) -> Tuple[str | None, List[str]]:
        all_rows: List[dict] = []
        processed_ids: List[str] = []

        for order in orders:
            rows = self._build_rows_for_order(order)
            if rows:
                all_rows.extend(rows)
                sc_id = order.get("sellercloud_order_id")
                if sc_id is not None:
                    processed_ids.append(str(sc_id))

        if not all_rows:
            print(f"No orders from {dropshipper_code} were tracked.")
            return None, processed_ids
        
        if rows:
            # ... existing all_rows.extend(rows) etc ...
            email = (order.get("invoice_email") or "").strip()
            if email and order.get("email_notifications"):
                self.orders_processed_by_email.setdefault(email, []).append(
                    order["purchase_order_number"]
                )
        
        ftp_folder = orders[0].get("ftp_folder_name", "")
        remote_path = self._remote_csv_path(ftp_folder, dropshipper_code)
        csv_bytes = self._rows_to_csv_bytes(all_rows)
        self.ftp.upload_bytes(csv_bytes, remote_path)
        return remote_path, processed_ids

    def run(self, by_dropshipper: Dict[str, List[dict]]) -> dict:
        remote_files: List[str] = []
        orders_processed: List[str] = []
        try:
            for dropshipper_code, orders in by_dropshipper.items():
                remote_path, processed_ids = self._process_one_dropshipper(dropshipper_code, orders)
                if remote_path:
                    remote_files.append(remote_path)
                orders_processed.extend(processed_ids)

            # write tracking numbers back to DB (only orders that have tracking set)
            if by_dropshipper:
                self.d_db.save_tracking_data(by_dropshipper)

            return {
                "files_uploaded": remote_files,
                "orders_processed": orders_processed,
                "errors": self.errors,
                "missing_tracking": self.errors.get("missing_tracking", []),
                "orders_processed_by_email": self.orders_processed_by_email,
            }
        finally:
            self._safe_close_ftp()