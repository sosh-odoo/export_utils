def fetch_contacts():
    query = """
    SELECT Id, Name, BillingStreet, BillingCity, BillingState, 
            BillingPostalCode, BillingCountry, BillingLatitude, BillingLongitude, 
            Phone, Website, Industry, Ownership, Description, Active__c 
    FROM Account
    """
    # removed billingstatecode
    return query

def fetch_employees():
    query = """
    SELECT Id, FirstName, LastName, AccountId, Email, Phone, MobilePhone, 
            Title, MailingStreet, MailingCity, MailingState, 
            MailingCountry, MailingPostalCode, MailingLatitude, MailingLongitude, 
            Languages__c, Salutation, Description
    FROM Contact
    """
    # removed mailingstatecode
    return query

def fetch_leads():
    query = """
    SELECT Id, Salutation, FirstName, LastName, Status, Company, Email, 
            Phone, Website, LeadSource, AnnualRevenue, OwnerId, 
            Country, State
    FROM Lead
    """
    return query

def fetch_opportunities():
    query = """
    SELECT Id, Name, StageName, CloseDate, Amount, AccountId, OwnerId, 
            CreatedDate, LastModifiedDate
    FROM Opportunity
    """
    return query

def fetch_products(standard_pricebook_id):
    query = f"""
            SELECT Id, Name, ProductCode, Description, IsActive, Family, 
                StockKeepingUnit,
                (SELECT Id, Product2Id, UnitPrice, Pricebook2Id, IsActive 
                    FROM PricebookEntries 
                    WHERE IsActive = TRUE 
                    AND Pricebook2Id = '{standard_pricebook_id}')
            FROM Product2
            WHERE IsActive = TRUE
        """
    return query

def fetch_orders():
    query = """
    SELECT Id, AccountId, Status, OrderNumber, EffectiveDate, PoNumber, 
            TotalAmount, BillingStreet, BillingCity, BillingState, 
            BillingPostalCode, BillingCountry, ShippingStreet, 
            ShippingCity, ShippingState, ShippingPostalCode, ShippingCountry
    FROM Order
    """
    return query

def fetch_order_lines():
    query = """
    SELECT Id, OrderId, Product2Id, Quantity, UnitPrice, TotalPrice, 
            ListPrice, ServiceDate, Description, CreatedDate, OrderItemNumber 
    FROM OrderItem
    """
    return query
