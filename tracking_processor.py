from typing import Dict, List, Optional
from dropship_db import DropshipDb
import io
import csv
from kramer_functions import FTPFileManager

shipping_methods: Dict[str, Dict[str, str]] = {
    "UPS Ground": {"name": "UPS", "code": "UPS"},
    "FEDEX Ground HD": {"name": "FedEx", "code": "FEDHD"},
}


class TrackingReportProcessor:
    def __init__(self, d_db: DropshipDb):
        self.d_db = d_db
        self.errors = {"failed_to_process": []}
        self.ftp: Optional[FTPFileManager] = None  # lazy-init
        self.orders_processed_by_email = {}
        self.ftp_folder = "dropshipper/test_customer/tracking"

    # -------------- FTP helpers --------------
    def _ensure_ftp(self) -> None:
        if self.ftp is None:
            self.ftp = FTPFileManager()

    def _safe_close_ftp(self) -> None:
        if self.ftp is None:
            return
        try:
            self.ftp.close()
        except EOFError:  # Server already closed connection; safe to ignore on shutdown
            pass
        except Exception:  # Swallow close-time errors
            pass
        finally:
            self.ftp = None

    def _build_rows_for_order(self, order: dict) -> List[dict]:
        tracking_number = order.get("tracking_number")
        raw_dt = order.get("ship_date") or order.get("tracking_date")
        ship_date = self._to_ymd(raw_dt)
        raw_ship_method = order.get("ship_method")
        mapping = shipping_methods.get(raw_ship_method, {})
        carrier_name = mapping.get("name", "")
        ship_method_code = mapping.get("code", "")
        display_ship_method = "Ground"  # legacy display

        rows: List[dict] = []
        for it in order.get("items", []):
            sku = it.get("sku")
            qty = it.get("quantity")
            rows.append(
                {
                    "po_number": order.get("purchase_order_number"),
                    "sku": sku,
                    "quantity": qty,
                    "carrier_name": carrier_name,
                    "ship_method": display_ship_method,
                    "ship_method_code": ship_method_code,
                    "ship_date": ship_date,
                    "tracking_number": tracking_number,
                }
            )
        return rows

    def _to_ymd(self, value) -> str:
        if value is None:
            return ""
        try:
            from datetime import date, datetime as dt

            if isinstance(value, (dt, date)):
                return value.strftime("%Y-%m-%d")
            s = str(value).strip().replace("/", "-")
            return s[:10]  # take the date portion if there is time attached
        except Exception:
            return ""

    def _rows_to_csv_bytes(self, rows: List[dict]) -> bytes:
        if not rows:
            return b""
        columns = [
            "po_number",
            "sku",
            "quantity",
            "carrier_name",
            "ship_method",
            "ship_method_code",
            "ship_date",
            "tracking_number",
        ]
        buf = io.StringIO(newline="")
        writer = csv.DictWriter(buf, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
        return buf.getvalue().encode("utf-8")

    def _remote_csv_path(self, ftp_folder: str, dropshipper_code: str) -> str:
        fname = f"{dropshipper_code}.csv"
        ftp_folder = (ftp_folder or "").strip("/ ")
        return f"/{ftp_folder}/{fname}" if ftp_folder else f"/{fname}"

    def _process_one_dropshipper(
        self, dropshipper_code: str, orders: List[dict]
    ) -> Optional[str]:

        if not orders:
            print(f"No orders from {dropshipper_code} were tracked.")
            return None

        all_rows: List[dict] = []

        for order in orders:
            rows = self._build_rows_for_order(order)
            if rows:
                all_rows.extend(rows)

        #! comment back in for production
        # ftp_folder_path = f"dropshipper/{ftp_folder}/tracking"
        ftp_folder_path = "dropshipper/test_customer/tracking"
        remote_path = self._remote_csv_path(ftp_folder_path, dropshipper_code)
        csv_bytes = self._rows_to_csv_bytes(all_rows)

        self._ensure_ftp()
        self.ftp.upload_bytes(csv_bytes, remote_path)
        return remote_path

    def run(self, by_dropshipper: Dict[str, List[dict]]) -> dict:
        remote_files: List[str] = []
        orders_processed: List[str] = []

        try:
            self._ensure_ftp()

            for dropshipper_code, orders in by_dropshipper.items():
                remote_path = self._process_one_dropshipper(dropshipper_code, orders)
                if not remote_path:
                    continue

                remote_files.append(remote_path)

                for o in orders:
                    po = o.get("purchase_order_number")
                    if po not in orders_processed:
                        orders_processed.append(po)

                notify_enabled = bool(orders[0].get("email_notifications", 0))
                notify_enabled = 1
                email = (orders[0].get("invoice_email") or "").strip()
                email = "rghanti@krameramerica.com"
                if notify_enabled and email:
                    pos = sorted(
                        {
                            o.get("purchase_order_number")
                            for o in orders
                            if o.get("purchase_order_number")
                        }
                    )
                    current = set(self.orders_processed_by_email.get(email, []))
                    current.update(pos)
                    self.orders_processed_by_email[email] = sorted(current)

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
