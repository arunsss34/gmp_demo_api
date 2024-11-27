import base64
import os
from fpdf import FPDF
from datetime import datetime as dt
import uuid


class PDF(FPDF):
    def __init__(self, agent_name=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.agent_name = agent_name
        self.first_page = True

    def header(self):
        if self.first_page:
            self.set_font('Arial', 'B', 16)
            self.cell(0, 10, 'Lucky Group Spinning Division', ln=True, align='C')
            self.set_font('Arial', 'B', 12)
            self.cell(0, 10, 'Lucky Yarn Tex India Private Limited', ln=True, align='C')
            self.set_font('Arial', 'B', 10)
            self.cell(0, 10, 'Pending Order - ' + str(self.agent_name), ln=True, align='C')
            self.set_font('Arial', 'B', 10)
            self.line(5, self.get_y(), 205, self.get_y())
            self.first_page = False
        self.set_line_width(0.1)
        self.rect(5, 5, 200, 277)
        self.set_font('Arial', 'B', 9)
        self.cell(20, 10, 'Date', border=0, align='L')
        self.cell(30, 10, 'Po No', border=0, align='L')
        self.cell(50, 10, 'Party Name', border=0, align='L')
        self.cell(30, 10, 'Agent Name', border=0, align='L')
        self.cell(15, 10, 'Price', border=0, align='L')
        self.cell(12, 10, 'Bags', border=0, align='L')
        self.cell(12, 10, 'Terms', border=0, align='L')
        self.cell(15, 10, 'Pending Bags', border=0, align='L')
        self.ln()
        self.line(5, self.get_y(), 205, self.get_y())
        self.ln(2)

    def footer(self):
        self.set_y(-15)
        self.set_font('Arial', 'I', 8)
        self.set_x(10)
        self.cell(0, 10, f"Page {self.page_no()} - {dt.now().strftime('%d-%m-%Y')}", 0, 0, 'L')

    def party_table(self, data):
        t_bag = 0
        tp_bag = 0
        for party in data:
            self.ln(1)
            self.set_font('Arial', 'B', 8)
            self.cell(0, 5, f"{party['item_description']}", ln=True, align='C')
            self.ln(1)
            self.set_font('Arial', '', 6)
            bug_t = 0
            p_bug_t = 0
            for index, invoice in enumerate(party['data']):
                bug_t = invoice['Subtotal Bags']
                p_bug_t = invoice['Subtotal PendingBags']
                t_bag = invoice['Total Bags']
                tp_bag = invoice['Total PendingBags']
                self.cell(20, 10, invoice['Date'], border=0, align='L')
                self.cell(30, 10, invoice['Po No'], border=0, align='L')
                self.cell(50, 10, invoice['Party Name'], border=0, align='L')
                self.cell(30, 10, invoice['Agent Name'], border=0, align='L')
                self.cell(15, 10, f"{invoice['Price']:.2f}", border=0, align='L')
                self.cell(12, 10, f"{invoice['Bags']:.2f}", border=0, align='L')
                self.cell(12, 10, invoice['Terms'], border=0, align='L')
                self.cell(15, 10, f"{invoice['PendingBags']:.2f}", border=0, align='R')
                self.set_text_color(0, 0, 0)
                self.ln()
            self.line(5, self.get_y(), 205, self.get_y())
            self.set_font('Arial', 'B', 8)
            self.cell(50, 10, f"", border=0, align='C')
            self.cell(24, 10, f"", border=0, align='C')
            self.cell(24, 10, f"", border=0, align='C')
            self.cell(24, 10, f"", border=0, align='C')
            self.cell(15, 10, f"Total:", border=0, align='C')
            self.cell(25, 10, bug_t, border=0, align='C')
            self.cell(25, 10, p_bug_t, border=0, align='R')
            self.ln()
            self.line(5, self.get_y(), 205, self.get_y())
        self.line(5, self.get_y(), 205, self.get_y())
        self.set_font('Arial', 'B', 8)
        self.cell(50, 10, f"", border=0, align='C')
        self.cell(24, 10, f"", border=0, align='C')
        self.cell(24, 10, f"", border=0, align='C')
        self.cell(24, 10, f"", border=0, align='C')
        self.cell(15, 10, f"Over All Total:", border=0, align='C')
        self.cell(25, 10, t_bag, border=0, align='C')
        self.cell(25, 10, tp_bag, border=0, align='R')
        self.ln()
        self.line(5, self.get_y(), 205, self.get_y())


def generate_pdf(data, agent_name):
    pdf = PDF(agent_name=agent_name)
    pdf.set_auto_page_break(auto=True, margin=25)
    pdf.add_page()
    pdf.party_table(data)
    base_dir = "./temp/"
    if not os.path.exists(base_dir):
        os.makedirs(base_dir)
    pdf_output_path = "Customer PO Details.pdf"
    pdf.output(pdf_output_path)
    base64_string = file_to_base64(pdf_output_path)
    return base64_string, pdf_output_path


def file_to_base64(file_path):
    try:
        with open(file_path, 'rb') as file:
            file_content = file.read()
            base64_encoded = base64.b64encode(file_content)
            base64_string = base64_encoded.decode('utf-8')
            return base64_string
    except Exception as e:
        return {"status": "error", "message": str(e)}
