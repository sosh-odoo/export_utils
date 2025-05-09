from odoo import models, fields, api
from odoo.exceptions import UserError

class TransferLog(models.Model):
    _name = 'transfer.log'
    _description = 'Transfer Log'

    name = fields.Char(string='Name', required=True)
    sync_status = fields.Selection([
        ('pending', 'Pending'),
        ('in_progress', 'In Progress'),
        ('completed', 'Completed'),
        ('failed', 'Failed')
    ], string='Sync Status', default='pending')
    transfer_category = fields.Selection([
        ('invoice', 'Invoice'),
        ('invoice_line', 'Invoice Line'),
        ('order', 'Order'),
        ('order_line', 'Order Line'),
        ('abandoned_cart', 'Abandoned Cart'),
        ('product', 'Product'),
        ('account', 'Account'),
        ('contact', 'Contact'),
        ('lead', 'Lead'),
        ('opportunity', 'Opportunity'),
    ], string='Transfer Category', required=True)
    source = fields.Char(string='Source', required=True)
    error_message = fields.Text(string='Error Message')
    transfer_date = fields.Datetime(string='Transfer Date', default=fields.Datetime.now)