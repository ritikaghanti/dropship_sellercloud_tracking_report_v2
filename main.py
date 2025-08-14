from dropship_db import DropshipDb
from seller_cloud_api import SellerCloudAPI
from sellercloud_helpers import get_orders_by_ids
from tracking_processor import TrackingReportProcessor
from email_helper import EmailHelper
from kramer_functions import ProcessLogger

def main():
    dropship_db = DropshipDb()

    # 1) Flat list from DB
    pending = dropship_db.get_untracked_orders()
    if not pending:
        print("No untracked purchase orders found.")
        return

    # 2) Enrich with SellerCloud and partition
    sc_api = SellerCloudAPI()
    by_dropshipper, cancelled, on_hold, problem = get_orders_by_ids(sc_api, pending)
    
    #Start of debugging
    def _peek(label, rows, limit=10):
        print(f"\n{label}: {len(rows)}")
        for r in rows[:limit]:
            print(f" - PO={r.get('purchase_order_number')}  "
                f"SC={r.get('sellercloud_order_id')}  "
                f"status={r.get('sc_status')}  "
                f"tracking={r.get('tracking_number')}")

    _peek("Cancelled", cancelled)
    _peek("OnHold", on_hold)
    _peek("ProblemOrder", problem)

    print("\nReady-to-process by dropshipper:")
    for code, orders in by_dropshipper.items():
        print(f" {code}: {len(orders)} orders")
        for r in orders[:5]:
            print(f"   - {r.get('purchase_order_number')}  "
                f"tracking={bool(r.get('tracking_number'))}")
    #End of debugging. Can be deleted if not required

    #Updates DB to mark orders as Cancelled or OnHold
    for o in cancelled:
        dropship_db.turning_on_is_cancelled_status(o["purchase_order_number"])
    for o in on_hold:
        dropship_db.turning_on_is_backorder_status(o["purchase_order_number"])

    if not by_dropshipper:
        print("No orders ready to track/upload after filtering statuses.")
        return

    # 3) Process uploads and save tracking numbers
    processor = TrackingReportProcessor(dropship_db, sc_api)
    result = processor.run(by_dropshipper)

    print(f"Processed {len(result['orders_processed'])} orders.")
    if result["files_uploaded"]:
        print("Uploaded files:")
        for p in result["files_uploaded"]:
            print(" -", p)

    missing = result.get("missing_tracking", [])
    if missing:
        print(f"{len(missing)} orders had no tracking:")
        for po in missing[:10]:
            print(" -", po)

    # 4) Emails
    emailer = EmailHelper(test_recipient="rghanti@krameramerica.com")
    emailer.send_tracking_confirmation(result.get("orders_processed_by_email", {}))
    emailer.send_error_summary(result["errors"])

if __name__ == "__main__":
    process_logger = ProcessLogger()
    try:
        main()
        process_logger.end("success")
    except Exception as e:
        error_message = f"An error occurred: {e}"
        print(error_message)
        # EmailHelper().send_exception_notification(error_message)
        process_logger.end("failure", error_message)
        raise e