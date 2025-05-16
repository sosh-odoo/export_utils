# Part of Odoo. See LICENSE file for full copyright and licensing details.

{
    "name": "Data Fetcher - Salesforce",
    "summary": """Fetch data from salesforce to odoo""",
    "category": "Technical Settings",
    "version": "1.0",
    "depends": ["data_fetcher_base"],
    "data": [
        "data/odoo_import_cron.xml",
        "views/portal_credentials_form.xml",
    ],
    'license': 'LGPL-3',
    'application': True
}
