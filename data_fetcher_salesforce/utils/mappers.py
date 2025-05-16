from typing import Dict, Any, List, Optional

def map_account_to_partner(sf_account: Dict[str, Any]) -> Dict[str, Any]:
    """
    Map Salesforce Account to Odoo res.partner
    """
    # Map ownership to is_public
    ownership = sf_account.get("Ownership")
    is_public = "false"
    if ownership:
        is_public = "public" in (ownership.lower() or "")
    
    return {
        "name": sf_account.get("Name"), 
        "phone": sf_account.get("Phone"),
        "website": sf_account.get("Website"),
        "ref": sf_account.get("Id"),
        "street": sf_account.get("BillingStreet"),
        "city": sf_account.get("BillingCity"),
        # "country_id/.id": sf_account.get("BillingCountry"),
        # "state_id/.id": sf_account.get("BillingState"),
        "zip": sf_account.get("BillingPostalCode"),
        "partner_latitude": sf_account.get("BillingLatitude"),
        "partner_longitude": sf_account.get("BillingLongitude"),
        # "industry_id/.id": sf_account.get("Industry"),
        "is_company": "true",
        "company_type": "company",
        "is_public": is_public,
        # Additional info can be stored in comment field or custom field
        "comment": sf_account.get("Description"),
    }

def map_contact_to_partner(sf_contact: Dict[str, Any]) -> Dict[str, Any]:
    """
    Map Salesforce Contact to Odoo res.partner
    """
    return {
        "name": f"{sf_contact.get('FirstName', '')} {sf_contact.get('LastName', '')}".strip(),
        "email": sf_contact.get("Email"),
        "phone": sf_contact.get("Phone"),
        "mobile": sf_contact.get("MobilePhone"),
        "ref": sf_contact.get("Id"),
        "function": sf_contact.get("Title"),  # Job title
        "type": 'contact',
        "street": sf_contact.get("MailingStreet"),
        "city": sf_contact.get("MailingCity"),
        # "country_id": sf_contact.get("MailingCountry"),
        # "state_id": sf_contact.get("MailingState"),
        "zip": sf_contact.get("MailingPostalCode"),
        # "title/.id": sf_contact.get("Salutation"),
        "partner_latitude": sf_contact.get("MailingLatitude"),
        "partner_longitude": sf_contact.get("MailingLongitude"),
        "employee": "true",
        "is_company": "false",
        "company_type": "person",
        "lang": sf_contact.get("Languages__c"),
        "comment": sf_contact.get("Description"),
        # "parent_id/.id": sf_contact.get("AccountId"),  # Used to link to parent company
    }

def map_lead_to_crm(sf_lead: Dict[str, Any]) -> Dict[str, Any]:
    """
    Map Salesforce Lead to Odoo crm.lead
    """
    return {
        "referred": sf_lead.get("Id"),
        "name": sf_lead.get("Company"),
        "type": "lead",
        "contact_name": f"{sf_lead.get('FirstName', '')} {sf_lead.get('LastName', '')}".strip(),
        "email_from": sf_lead.get("Email"),
        "phone": sf_lead.get("Phone"),
        "website": sf_lead.get("Website"),
        # title will be resolved by the import process
        # source_id will be resolved by the import process
        "active": "true", # always true to make record accessible
        # expected_revenue will be calculated based on AnnualRevenue if present
        "description": sf_lead.get("Description"),
    }

def map_opportunity_to_crm(sf_opportunity: Dict[str, Any]) -> Dict[str, Any]:
    """
    Map Salesforce Opportunity to Odoo crm.lead(opportunity)
    """
    return {
        "referred": sf_opportunity.get("Id"),
        "name": sf_opportunity.get("Name"),
        "type": "opportunity",
        # partner_id will be resolved by the import process
        "date_deadline": sf_opportunity.get("CloseDate"),
        "expected_revenue": sf_opportunity.get("Amount", 0.0),
        "probability": sf_opportunity.get("Probability", 0.0),
        # stage_id will be resolved by the import process
        "active": "true", # always true to make record accessible
        "ref": sf_opportunity.get("AccountId") 
    }

def map_product_to_odoo(sf_product: Dict[str, Any]) -> Dict[str, Any]:
    """
    Map Salesforce Product + default(0th index) PricebookEntry to Odoo product.template
    """
    # product_type = "consu" if sf_product.get("Type") == "Goods" else "service"
    product_type = "consu" if sf_product.get("SBQQ__ChargeType__c") == "One-Time" else "service"
    pricebook_entries = sf_product.get("PricebookEntries", {}).get("records", [])
    list_price = pricebook_entries[0]["UnitPrice"] if pricebook_entries else 0.0

    return {
        "description": sf_product.get("Id"),
        "name": sf_product.get("Name", ""),
        "default_code": sf_product.get("ProductCode", ""),
        "description_sale": sf_product.get("Description", ""),
        "active": "true" if sf_product.get("IsActive") else "false",
        "type": product_type,
        "list_price": list_price
    }

def map_order_to_odoo(sf_order: Dict[str, Any]) -> Dict[str, Any]:
    """
    Map Salesforce Order to Odoo sale.order
    """
    # Map Salesforce status to Odoo state
    state = "sale" if sf_order.get("Status") == "Activated" else "sent"

    return {
        "reference": sf_order.get("Id"),
        "name": sf_order.get("OrderNumber", ""),
        "date_order": sf_order.get("EffectiveDate"),
        "client_order_ref": sf_order.get("PoNumber", ""),
        "amount_total": sf_order.get("TotalAmount", 0.0),
        "state": state,
        # partner_id will be resolved by the import process
    }

def map_order_line_to_odoo(sf_order_item: Dict[str, Any]) -> Dict[str, Any]:
    """
    Map Salesforce Order Item to Odoo sale.order.line
    """
    return {
        "virtual_id": sf_order_item.get("Id"),
        # "name": sf_order_item.get("Description", ""),
        "product_uom_qty": sf_order_item.get("Quantity", 1.0),
        "price_unit": sf_order_item.get("UnitPrice", 0.0),
        "price_subtotal": sf_order_item.get("TotalPrice", 0.0),
        # product_id will be resolved by the import process
        # order_id will be resolved by the import process
    }
