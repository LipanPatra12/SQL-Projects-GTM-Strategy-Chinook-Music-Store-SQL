-- establishing relationship between customer and employee tables

ALTER TABLE customer
ADD CONSTRAINT FK_customer_employee
FOREIGN KEY (support_rep_id)
REFERENCES employee(employee_id);


-- establishing relationship between customer and invoice tables
ALTER TABLE invoice
ADD CONSTRAINT FK_invoice_customer
FOREIGN KEY (customer_id)
REFERENCES customer(customer_id);


-- establishing relationship between invoice and invoice_line tables
ALTER TABLE invoice_line
ADD CONSTRAINT FK_invoice_invoiceline
FOREIGN KEY (invoice_id)
REFERENCES invoice(invoice_id);


-- establishing relationship between invoice_line and track tables
ALTER TABLE invoice_line
ADD CONSTRAINT FK_invoiceline_track
FOREIGN KEY (track_id)
REFERENCES track(track_id);