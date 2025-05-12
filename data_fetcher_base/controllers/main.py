from odoo import http
from odoo.http import request
import logging

logger = logging.getLogger(__name__)

class SyncController(http.Controller):

    @http.route('/my/import', type='http', auth='user', website=True, csrf=False)
    def credentials_form(self, **kwargs):
        module_model = request.env['ir.module.module'].sudo()
        installed_modules = module_model.search([
            ('state', '=', 'installed'),
            ('name', 'like', 'data_fetcher_%')
        ])

        # Extract system names from module names
        systems = [
            m.name.replace('data_fetcher_', '')
            for m in installed_modules
            if m.name != 'data_fetcher_base'
        ]
        return request.render('data_fetcher_base.portal_credentials_form_base', {
            'systems': systems,
        })
    
    @http.route('/my/api/data_transfer', type='http', auth='user', csrf=True)
    def transfer_data(self, **kwargs):
        """Main endpoint for all data transfers"""
        service_type = kwargs.get('system')
        if not service_type:
            return self.transfer_error("No service type specified")
            
        # Store common Odoo credentials
        self._store_odoo_credentials(kwargs)
            
        # Dispatch to the appropriate handler method
        handler_method = f"_handle_{service_type}"
        if hasattr(self, handler_method):
            return getattr(self, handler_method)(kwargs)
        else:
            return self.transfer_error(f"Unsupported service: {service_type}")
    
    def _store_odoo_credentials(self, data):
        """Store Odoo credentials in system parameters"""
        config = request.env['ir.config_parameter'].sudo()
        config.set_param('odoo.url', data.get('odoo_url'))
        config.set_param('odoo.db', data.get('odoo_db'))
        config.set_param('odoo.username', data.get('odoo_username'))
        config.set_param('odoo.password', data.get('odoo_api_key'))
    
    def transfer_success(self):
        return request.render('data_fetcher_base.portal_import_data_result', {
            'results': 'Transfer completed successfully',
        })
        
    def transfer_error(self, message="Unknown error occurred"):
        """Return error response"""
        return request.render('data_fetcher_base.portal_import_data_result', {
            'error': message
        })