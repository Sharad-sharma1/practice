#!/usr/bin/env python3
import sys
import smtplib
import pandas as pd
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import snowflake.connector
# from credentials import sender_email, to_email,cc_email, app_password, user, password, account, server

user='avdhutG'
password='Powai@1234'
account='PHARMARACK-ANALYTICS'
sender_email = "pranalytics.support@pharmarack.com"
app_password= 'Yoc28153'
server='smtp.office365.com'
to_email = ["swardaj@pharmarack.com","amit.kaul@pharmarack.com", "Vishakha.shah@pharmarack.com","avdhut.ghatage@pharmarack.com"]
cc_email = ["aarifp@pharmarack.com", "syedm@pharmarack.com", "Sujatag@pharmarack.com", "Hiren.Karnani@pharmarack.com", "Surya.Choudhury@pharmarack.com", "suryas@pharmarack.com", "ramasamy.ramar@pharmarack.com", "shrutika.wagh@pharmarack.com", "poojag@pharmarack.com", "isha.rathi@pharmarack.com", "Dhruv.Gulati@pharmarack.com"]

# "swardaj@pharmarack.com","amit.kaul@pharmarack.com", "Vishakha.shah@pharmarack.com"
# "aarifp@pharmarack.com", "syedm@pharmarack.com", "Sujatag@pharmarack.com", "Hiren.Karnani@pharmarack.com", "Surya.Choudhury@pharmarack.com", "suryas@pharmarack.com", "ramasamy.ramar@pharmarack.com", "shrutika.wagh@pharmarack.com", "poojag@pharmarack.com", "isha.rathi@pharmarack.com", "Dhruv.Gulati@pharmarack.com"

# Connect to Snowflake using a context manager
def fetch_data_from_snowflake(query):
    with snowflake.connector.connect(user=user, password=password, account=account) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchall()

# Function to format numbers with commas
def format_number(value):
    return "{:,}".format(value)

# Function to generate an HTML table from query results
def generate_html_table(columns, data, title, extra_info=""):
    table_html = f"""
    <html>
      <body>
        <p>{title}</p>  <!-- Title added here -->
        <p>{extra_info}</p>
        <table border="1" cellpadding="8" cellspacing="0" style="border-collapse: collapse; width: 100%; text-align: center;">
          <thead>
            <tr style="background-color: #f2f2f2; font-weight: bold;">
    """
    if not data:
      table_html += "<html><body><p>No data for yesterday</p></body></html>"
    else:
      # Add headers
      for column in columns:
          table_html += f"<th style='border: 1px solid black;'>{column}</th>"
      
      table_html += "</tr></thead><tbody>"

      # Add rows
      for row in data:
          table_html += "<tr>"
          for cell in row:
              table_html += f"<td style='border: 1px solid black;'>{cell}</td>"
          table_html += "</tr>"
      
      table_html += "</tbody></table></body></html>"
    return table_html

# BULK MAPPING DATA QUERY 1
query_1 = """
SELECT TO_DATE(A.RECORD_DATE) RECORD_DATE,
       B.USERTYPE,
       COUNT(DISTINCT CASE WHEN CURRENT_ALLOCATION='Bulk Mapping' THEN USERNAME END) AS USERS_COUNT,
       COUNT(CASE WHEN CURRENT_ALLOCATION='Bulk Mapping' THEN DISTRIBUTOR_CODE END) AS BULK_MAPPED_COUNT,
       COUNT(DISTINCT CASE WHEN CURRENT_ALLOCATION='Bulk Mapping' THEN MDM_PRODUCT_CODE END) AS BULK_MAPPED_PRODUCTS,
       ROUND((BULK_MAPPED_COUNT / REPLACE(USERS_COUNT, 0, 1))) MAPPING_PER_USER,
       ROUND((BULK_MAPPED_PRODUCTS / REPLACE(USERS_COUNT, 0, 1))) PRODUCT_PER_USER
FROM (
    SELECT DISTRIBUTOR_CODE, PRODUCT_CODE, PRODUCT_NAME, PACK, REGEX, MDM_PRODUCT_CODE, CREATED_BY, RECORD_DATE
    FROM PHARMMDM.MASTERDATA.MDM_DIST2PROD_MANUAL_MAPPING
    UNION ALL
    SELECT DISTRIBUTOR_CODE, PRODUCT_CODE, PRODUCT_NAME, PACK, REGEX, MDM_PRODUCT_CODE, CREATED_BY, RECORD_DATE
    FROM PHARMMDM.MASTERDATA.MDM_DIST2PROD_MANUAL_UPLOAD
) A
INNER JOIN SANDBOX_DB.SANDBOX_ANALYST.AVDHUT_18DEC_DATA_STEWARDS B 
    ON A.CREATED_BY = B.USERNAME AND TO_DATE(A.RECORD_DATE) = B.RECORD_DATE
WHERE TO_DATE(A.RECORD_DATE) = DATEADD(DAY, -1, CURRENT_DATE)
GROUP BY ALL
ORDER BY TO_DATE(A.RECORD_DATE) DESC, USERTYPE;
"""

data1 = fetch_data_from_snowflake(query_1)
columns1 = ['RECORD_DATE', 'USERTYPE', 'USERS_COUNT', 'BULK_MAPPED_COUNT', 'BULK_MAPPED_PRODUCTS', 'MAPPING_PER_USER', 'PRODUCT_PER_USER']
df1 = pd.DataFrame(data1, columns=columns1)

# BULK MAPPING DATA QUERY 2
query_2 = """
SELECT 
    COUNT(CASE WHEN CURRENT_ALLOCATION='Bulk Mapping' THEN DISTRIBUTOR_CODE END) AS BULK_MAPPED_COUNT,
    COUNT(DISTINCT CASE WHEN CURRENT_ALLOCATION='Bulk Mapping' THEN MDM_PRODUCT_CODE END) AS BULK_MAPPED_PRODUCTS
FROM (
    SELECT DISTRIBUTOR_CODE, PRODUCT_CODE, PRODUCT_NAME, PACK, REGEX, MDM_PRODUCT_CODE, CREATED_BY, RECORD_DATE
    FROM PHARMMDM.MASTERDATA.MDM_DIST2PROD_MANUAL_MAPPING
    UNION ALL
    SELECT DISTRIBUTOR_CODE, PRODUCT_CODE, PRODUCT_NAME, PACK, REGEX, MDM_PRODUCT_CODE, CREATED_BY, RECORD_DATE
    FROM PHARMMDM.MASTERDATA.MDM_DIST2PROD_MANUAL_UPLOAD
) A
INNER JOIN SANDBOX_DB.SANDBOX_ANALYST.AVDHUT_18DEC_DATA_STEWARDS B 
    ON A.CREATED_BY = B.USERNAME AND TO_DATE(A.RECORD_DATE) = B.RECORD_DATE
WHERE TO_DATE(A.RECORD_DATE) >= '2024-12-07'  
    AND TO_DATE(A.RECORD_DATE) <= DATEADD(DAY, -1, CURRENT_DATE)
GROUP BY ALL;
"""

data1A = fetch_data_from_snowflake(query_2)
columns1A = ['BULK_MAPPED_COUNT', 'BULK_MAPPED_PRODUCTS']
df1A = pd.DataFrame(data1A, columns=columns1A)

# Define the formatted values
mapped_count_til_date = format_number(df1A.iloc[0]['BULK_MAPPED_COUNT'])
unique_product_mapped_til_date = format_number(df1A.iloc[0]['BULK_MAPPED_PRODUCTS'])

# HTML for BULK MAPPING REPORT
table_html_1 = generate_html_table(
    columns1, 
    data1, 
    "Hi All,<br><br><strong>Bulk Mapping Report:</strong>", 
    f"Mapped Count till Date: <strong>{mapped_count_til_date}</strong><br>Unique Products Mapped till Date: <strong>{unique_product_mapped_til_date}</strong>"
)

# BULK QC DATA QUERY
query_3 = """
SELECT TO_DATE(RECORD_DATE) RECORD_DATE,
       COUNT(DISTINCT QCED_BY) USER_COUNT,
       COUNT(1) TOTAL_QCED,
       COUNT(CASE WHEN QC_FLAG=1 THEN 1 END) QC_PASSED,
       COUNT(CASE WHEN QC_FLAG=2 THEN 1 END) QC_FAILED
FROM PHARMMDM.MASTERDATA.MDM_DIST2PROD_MANUAL_AUDIT_QC
WHERE TO_DATE(RECORD_DATE) = DATEADD(DAY, -1, CURRENT_DATE)
GROUP BY ALL
ORDER BY TO_DATE(RECORD_DATE) DESC;
"""

data2 = fetch_data_from_snowflake(query_3)
columns2 = ['RECORD_DATE', 'USER_COUNT', 'TOTAL_QCED', 'QC_PASSED', 'QC_FAILED']
df2 = pd.DataFrame(data2, columns=columns2)

if data1 == data2:
    print("Data1 and Data2 are the same. Terminating the script.")
    sys.exit()

# BULK QC TILL DATE QUERY
query_4 = """
SELECT
    COUNT(1) TOTAL_QCED,
    COUNT(CASE WHEN QC_FLAG=1 THEN 1 END) QC_PASSED,
    ROUND((QC_PASSED / TOTAL_QCED) * 100) AS QC_PASSED_PERCENTAGE
FROM PHARMMDM.MASTERDATA.MDM_DIST2PROD_MANUAL_AUDIT_QC
WHERE TO_DATE(RECORD_DATE) < CURRENT_DATE;
"""

data2A = fetch_data_from_snowflake(query_4)
columns2A = ['TOTAL_QCED', 'QC_PASSED', 'QC_PASSED_PERCENTAGE']
df2A = pd.DataFrame(data2A, columns=columns2A)

# Define the formatted values
qced_count_til_date = format_number(df2A.iloc[0]['TOTAL_QCED'])
qc_passed_percentage_til_date = format_number(df2A.iloc[0]['QC_PASSED_PERCENTAGE'])

# HTML for BULK QC REPORT
table_html_2 = generate_html_table(
    columns2, 
    data2, 
    "<strong>Bulk QC Report:</strong>", 
    f"QCED Count till Date: <strong>{qced_count_til_date}</strong><br>QC Passed Percentage till Date: <strong>{qc_passed_percentage_til_date}%</strong>"
)

# Email Setup
def send_email(subject, body):
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = ", ".join(to_email)
    msg['Cc'] = ", ".join(cc_email)
    msg['Subject'] = subject
    msg['In-Reply-To'] = '<PN3P287MB2779CA41DD373A039733C5789CE62@PN3P287MB2779.INDP287.PROD.OUTLOOK.COM>'
    msg['References'] = '<PN3P287MB2779CA41DD373A039733C5789CE62@PN3P287MB2779.INDP287.PROD.OUTLOOK.COM>'
    msg.attach(MIMEText(body, 'html'))

    recipients = to_email + cc_email

    try:
        with smtplib.SMTP(server, 587) as s:
            s.starttls()
            s.login(sender_email, app_password)
            s.sendmail(sender_email, recipients, msg.as_string())
        print("Email sent successfully.")
    except Exception as e:
        print(f"Error sending email: {e}")

# Combine both reports and send the email
combined_html = table_html_1 + table_html_2
send_email("Report on Bulk Mapping Screen and Bulk QC screen", combined_html)
