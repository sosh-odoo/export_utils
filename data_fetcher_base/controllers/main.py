from odoo import http
from odoo.http import request

class ImportController(http.Controller):

    @http.route('/my/imports', type='http', auth='user', website=True)
    def list_imports(self, **kwargs):
        name = request.env.user.name
        logs = request.env['transfer.log'].sudo().search([
            ('name', '=', name )
        ], order='create_date desc')
        return request.render('data_fetcher_base.portal_import_list', {
            'logs': logs,
        })

    @http.route('/my/imports/new', type='http', auth='user', website=True, csrf=False)
    def credentials_form(self, **kwargs):
        module_model = request.env['ir.module.module'].sudo()
        installed_modules = module_model.search([
            ('state', '=', 'installed'),
            ('name', 'like', 'data_fetcher_%')
        ])
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
        service_type = kwargs.get('system')
        if not service_type:
            return self.transfer_error("No service type specified")

        handler_method = f"_handle_{service_type}"
        if hasattr(self, handler_method):
            return getattr(self, handler_method)(kwargs)
        else:
            return self.transfer_error(f"Unsupported service: {service_type}")

    def transfer_success(self):
        return request.render('data_fetcher_base.portal_import_data_result', {
            'results': 'Transfer completed successfully',
        })

    def transfer_error(self, message="Unknown error occurred"):
        return request.render('data_fetcher_base.portal_import_data_result', {
            'error': message
        })
