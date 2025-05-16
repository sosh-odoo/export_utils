from odoo import models, fields, api
from odoo.exceptions import UserError

class TransferLog(models.Model):
    _name = 'transfer.log'
    _description = 'Import Transfer Log'
    
    name = fields.Char(string='Name', required=True)
    db_name = fields.Char(string='Database Name', default=None)
    db_user = fields.Char(string='Database User', default=None)
    db_url = fields.Char(string='Database URL', default=None)
    db_password = fields.Char(string='Database Password', default=None)
    import_status = fields.Selection([
        ('pending', 'Pending'),
        ('in_progress', 'In Progress'),
        ('completed', 'Completed'),
        ('failed', 'Failed')
    ], string='Import Status', default='pending')
    source = fields.Char(string='Source', required=True)
    error_message = fields.Text(string='Error Message')
    transfer_date = fields.Datetime(string='Transfer Date', default=fields.Datetime.now)
    import_date = fields.Datetime(string='Import Date')
    attachment_ids = fields.One2many(
        'ir.attachment', 'res_id',
        domain=[('res_model', '=', 'transfer.log')],
        string='Attachments',
        readonly=True
    )
