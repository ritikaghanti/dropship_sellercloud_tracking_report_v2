# email_helper.py
from __future__ import annotations

from typing import Dict, List
from pathlib import Path
from jinja2 import Template
from kramer_functions import GmailNotifier, AzureSecrets


class EmailHelper:
    """
    Sends:
      - tracking confirmation emails to recipients with their PO list (HTML)
      - error summaries to IT
      - exception notifications to IT

    Relies on:
      - GmailNotifier (credentials via Azure Key Vault)
      - Secret "email-address-it-department" for IT notifications
    """

    def __init__(self, template_name: str = "tracking_email_template.html", test_recipient: str | None = None):
        self.notifier = GmailNotifier()
        self.secrets = AzureSecrets()
        self.it_email = self.secrets.get_secret("email-address-it-department")
        self.reply_to = "orders@krameramerica.com"
        self.test_recipient = test_recipient
        # Resolve template path next to this file by default
        self.template_path = Path(__file__).parent / template_name
        if not self.template_path.exists():
            # You can change this to raise if you want hard failure
            print(f"[EmailHelper] Warning: template not found at {self.template_path}")

    # ---------- Public API ----------
    def _resolve_recipients(self, real_recipients: list[str]) -> list[str]:
        return [self.test_recipient] if self.test_recipient else real_recipients

    def send_tracking_confirmation(self, orders_by_email: Dict[str, List[str]]) -> None:
        """
        orders_by_email: { "recipient@example.com": ["PO-123", "PO-456", ...], ... }
        """
        if not orders_by_email:
            return

        template = self._load_template()

        for email, po_list in orders_by_email.items():
            if not po_list:
                continue

            html = template.render(orders=po_list)

            self.notifier.send_notification(
                subject="[TEST] Kramer America Tracking Confirmation",
                body="Your orders have been processed and tracking info is ready.",
                # recipients=[email],
                recipients=self._resolve_recipients([email]),
                html_body=html,
                reply_to=self.reply_to,
                machine_info=False,
                # discord_notification=False,
            )

    def send_error_summary(self, errors: Dict[str, List]) -> None:
        """
        errors: the dict your processor returns, e.g. {"failed_to_process": [...], ...}
        Sends a plain-text summary to IT if there is anything to report.
        """
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
        for key, orders in errors.items():
            if not orders:
                continue

            label = label_map.get(key, key)
            lines.append(f"{label} ({len(orders)}):")

            for o in orders:
                if isinstance(o, dict):
                    po = (
                        o.get("purchase_order_number")
                        or o.get("sellercloud_order_id")
                        or o.get("id")
                        or ""
                    )
                    line = f"- {po}" if po else f"- {o}"
                else:
                    # handle str/int/None, etc.
                    line = f"- {o}"

                lines.append(line)

            lines.append("")  # blank line between sections

    def send_exception_notification(self, error_message: str) -> None:
        """
        Call this from your top-level except block.
        """
        self.notifier.send_notification(
            subject="Tracking Report Failure Notification",
            body=error_message,
            recipients=[self.it_email],
            machine_info=True,
        )

    # ---------- Internals ----------

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
