from odoo import fields, models

class SalesforceOdooMapping(models.Model):
    _name = 'salesforce.odoo.mapping'
    _description = 'Salesforce to Odoo ID Mapping'
    
    salesforce_id = fields.Char('Salesforce ID', required=True, index=True)
    odoo_id = fields.Integer('Odoo ID', required=True)
    model = fields.Char('Model Name', required=True, index=True)
    
    # _sql_constraints = [
    #     ('unique_mapping', 'unique(salesforce_id, model)', 'Mapping must be unique per model and Salesforce ID')
    # ]
