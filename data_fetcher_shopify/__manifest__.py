{
    'name': 'Data Fetcher - Shopify',
    'summary': 'Import data from Shopify',
    'version': '18.0',
    'depends': ['data_fetcher_base'],
    'author': 'CodeX',
    'category': 'Import Data',
    'website': 'https://www.odoo.com',
    'description': ' Import customers, products and orders from Shopify to Odoo',
    'data': [
            'data/odoo_import_cron.xml',
            'views/portal_credentials_form.xml',
        ],
    'installable': True,
    'application': True,
    'license': 'LGPL-3',
}
