# email_helper.py
from __future__ import annotations
from typing import Dict, List
from pathlib import Path
from jinja2 import Template
from kramer_functions import GmailNotifier, AzureSecrets

class EmailHelper:
    def __init__(self, template_name: str = "tracking_email_template.html", test_recipient: str | None = None):
        self.notifier = GmailNotifier()
        self.secrets = AzureSecrets()
        self.it_email = self.secrets.get_secret("email-address-it-department")
        self.reply_to = "orders@krameramerica.com"
        self.test_recipient = test_recipient  # if set, all emails go only to this address


        self.template_path = Path(__file__).parent / template_name
        if not self.template_path.exists():
            # You can change this to raise if you want hard failure
            print(f"[EmailHelper] Warning: template not found at {self.template_path}")


    def _resolve_recipients(self, real_recipients: List[str]) -> List[str]:
        return [self.test_recipient] if self.test_recipient else real_recipients

    def _load_template(self) -> Template:
        if not self.template_path.exists():
            # minimal inline fallback if file missing
            fallback = """
            <!DOCTYPE html>
            <html><body>
                <p>The following purchase order(s) have been processed:</p>
                <ul>{% for order in orders %}<li>{{ order }}</li>{% endfor %}</ul>
            </body></html>
            """
            return Template(fallback)
        with open(self.template_path, "r", encoding="utf-8") as f:
            return Template(f.read())


    def send_tracking_confirmation(self, orders_by_email: Dict[str, List[str]]) -> None:

        if not orders_by_email:
            return

        template = self._load_template()

        for email, po_list in orders_by_email.items():
            if not po_list:
                continue

            html = template.render(orders=po_list)
            recipients = self._resolve_recipients([email])

            self.notifier.send_notification(
                subject="Kramer America Tracking Confirmation",
                body="Your orders have been processed and tracking info is ready.",
                recipients=recipients,
                html_body=html,
                reply_to=self.reply_to,
                machine_info=False,
                discord_notification=False,  # explicitly disable Discord
            )

    def send_error_summary(self, errors: Dict[str, List]) -> None:
        if not errors or not any(errors.values()):
            return

        label_map = {
            "missing_sku": "Missing SKU in SellerCloud",
            "missing_shipping_cost": "Missing Shipping Cost",
            "failed_to_process": "Failed to Process Order",
            "failed_to_put_on_hold": "Failed to Put On Hold",
            "missing_tracking": "Missing Tracking Number",
        }

        lines: List[str] = []
        for key, items in errors.items():
            if not items:
                continue
            label = label_map.get(key, key)
            lines.append(f"{label} ({len(items)}):")
            for o in items:
                po = None
                if isinstance(o, dict):
                    po = (
                        o.get("purchase_order_number")
                        or o.get("sellercloud_order_id")
                        or o.get("id")
                    )
                lines.append(f"- {po if po else o}")
            lines.append("")

        recipients = self._resolve_recipients([self.it_email])
        self.notifier.send_notification(
            subject="Tracking Report Error Summary",
            body="\n".join(lines) if lines else "No details.",
            recipients=recipients,
            machine_info=True,
            discord_notification=False,
        )

    def send_exception_notification(self, error_message: str) -> None:
        recipients = self._resolve_recipients([self.it_email])
        self.notifier.send_notification(
            subject="Tracking Report Failure Notification",
            body=error_message,
            recipients=recipients,
            machine_info=True,
            discord_notification=False,
        )