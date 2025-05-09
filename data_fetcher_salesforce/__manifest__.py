# Part of Odoo. See LICENSE file for full copyright and licensing details.

{
    "name": "Data Fetcher - Salesforce",
    "summary": """Fetch data from salesforce to odoo""",
    "category": "Technical Settings",
    "version": "1.0",
    "depends": ["data_fetcher_base"],
    "data": [
        "data/sync_cron.xml",
        # "security/ir.model.access.csv",
        "views/portal_credentials_form.xml",
        # 'views/salesforce_transfer_log.xml',
    ],
    'license': 'LGPL-3',
}
