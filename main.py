from dropship_db import DropshipDb
from seller_cloud_api import SellerCloudAPI
from sellercloud_helpers import get_orders_by_ids
from tracking_processor import TrackingReportProcessor
from email_helper import EmailHelper
from kramer_functions import ProcessLogger


def main():
    dropship_db = DropshipDb()
    pending = dropship_db.get_untracked_orders()
    if not pending:
        print("No untracked purchase orders found.")
        return

    sc_api = SellerCloudAPI()
    by_dropshipper, cancelled, on_hold, problem = get_orders_by_ids(sc_api, pending)

    # Updates DB to mark orders as Cancelled or OnHold
    for o in cancelled:
        dropship_db.turning_on_is_cancelled_status(o["purchase_order_number"])
    for o in on_hold:
        dropship_db.turning_on_is_backorder_status(o["purchase_order_number"])

    # TODO when we complete this project make sure to add tests for the 2 above - dan note
    if not by_dropshipper:
        print("No orders ready to track/upload after filtering statuses.")
        return

    processor = TrackingReportProcessor(dropship_db)
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

    TEST_MODE = True
    TEST_EMAIL = "rghanti@krameramerica.com"
    emailer = EmailHelper(test_recipient=TEST_EMAIL)
    if TEST_MODE:
        # consolidate every processed PO into a single email to rghanti
        all_pos = sorted(set(result.get("orders_processed", [])))
        orders_by_email = {TEST_EMAIL: all_pos} if all_pos else {}
    else:
        orders_by_email = result.get("orders_processed_by_email", {})
    emailer.send_tracking_confirmation(orders_by_email)
    emailer.send_error_summary(result["errors"])
    if problem:
        emailer.send_problem_orders_alert(problem)


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
