{
    'name': 'Data Fetcher - Shopify',
    'summary': 'Sync products and orders with Shopify',
    'version': '18.0',
    'depends': ['data_fetcher_base'],
    'author': 'CodeX',
    'category': 'E-commerce',
    'website': 'https://www.odoo.com',
    'description': ' Sync products and orders from Shopify to Odoo',
    'data': [
            # 'security/ir.model.access.csv',
            'data/odoo_import_cron.xml',
            'views/portal_credentials_form.xml',
            # 'views/shopify_transfer_log.xml',
        ],
    'installable': True,
    'application': True,
    'license': 'LGPL-3',
}
