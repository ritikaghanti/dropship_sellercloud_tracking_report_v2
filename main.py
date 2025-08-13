from dropship_db import DropshipDb
from seller_cloud_api import SellerCloudAPI
from sellercloud_helpers import get_orders_by_ids
from tracking_processor import TrackingReportProcessor
from email_helper import EmailHelper
from kramer_functions import ProcessLogger


def main():
    dropship_db = DropshipDb()
    # Getting the untracked orders from the database
    untracked_orders, sellercloud_order_ids = dropship_db.get_untracked_orders()
    
    if not untracked_orders or not sellercloud_order_ids:
        print("No untracked purchase orders found.")
        return
    
    sc_api = SellerCloudAPI()
    sellercloud_orders = get_orders_by_ids(sc_api, sellercloud_order_ids)
    
    if not sellercloud_orders:
        print("There are no matching orders in SellerCloud.")
        return
    
    processor = TrackingReportProcessor(dropship_db, sc_api)
    result = processor.run(untracked_orders, sellercloud_orders)
    # result = {"files_uploaded": [...], "orders_processed": [...], "errors": {...}}
    print(f"Processed {len(result['orders_processed'])} orders.")

    if result["files_uploaded"]:
        print("Uploaded files:")
        for p in result["files_uploaded"]:
            print(" -", p)

    missing = result.get("missing_tracking", [])
    if missing:
        print(f"{len(missing)} orders had no tracking:")
        for po in missing[:10]:  # show first 10 for brevity
            print(" -", po)

    print(f"Processed {len(result['orders_processed'])} orders.")
    
    # emailer = EmailHelper()
    # emailer.send_tracking_confirmation(result.get("orders_processed_by_email", {}))
    # emailer.send_error_summary(result["errors"])
    emailer = EmailHelper(test_recipient="rghanti@krameramerica.com")
    emailer.send_tracking_confirmation(result.get("orders_processed_by_email", {
    "placeholder@nowhere": [f"PO-{i}" for i in range(1, 6)]
    }))
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