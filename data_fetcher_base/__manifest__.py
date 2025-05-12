{
    'name': 'Data Fetcher',
    'version': '1.0',
    'category': 'Import Data',
    'summary': 'Fetch data from other platforms',
    'description': """
        This module fetchs data from other platforms like salesforce, shopify, etc.
    """,
    'author': 'Odoo Developer 101',
    'depends': ['portal'],
    'images': ['static/description/icon.png'],
    'data': [
        'security/ir.model.access.csv',
        'views/portal_credentials_form.xml',
        'views/transfer_log.xml'
    ],
    'installable': True,
    'application': True,
    'license': 'LGPL-3',
}