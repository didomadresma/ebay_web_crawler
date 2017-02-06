#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import cgi
import html
from BackendTools.BackendProcessor import BackendProcessor

pattern = """
<!DOCTYPE HTML>
<html>
<head>
<meta charset="utf-8">
<title>Crawler</title>
</head>
<body>
    <center>
    <br>
    <img src="https://lh3.googleusercontent.com/-W10280gFbLQ/VvkqivGfwAI/AAAAAAAAPhk/W1Mc0mI00F46REY12W3rWbWx9tvhrcBAACCo/s144-Ic42/webCrawler.jpg" height="25" width="144" />
    <br>
    <br>
    <tr><td>
    Enter your eBay keywords
    <br>
    <br>
    <form action="/cgi-bin/crawler.py">
        <textarea name="text"></textarea>
        <input type="hidden" name="action" value="publish">
        <br>
        <input type="submit" value="Search" align="middle">
    </form>
    </td></tr>
    <br>
    {posts}
    </center>
</body>
</html>
"""

table = """
    <table style="width:80%" border="1">
        <tr>
            <th> # </th>
            <th> ITEM_ID </th>
            <th> TITLE </th>
            <th> CURRENT_PRICE </th>
            <th> CURRENCY </th>
            <th> PAYMENT_METHOD </th>
        </tr>
        <tr>
            <td> {line} </td>
            <td> {id} </td>
            <td> {title} </td>
            <td> {price} </td>
            <td> {currency} </td>
            <td> {payment_method} </td>
        </tr>
    </table>
    <table style="width:40%">
        <tr>
            <th> {results} </th>
        </tr>
    </table>
"""

bp = BackendProcessor()

form = cgi.FieldStorage()
action = form.getfirst("action", "")
output = ""

if action == "publish":
    text = html.escape(form.getfirst("text", ""))
    ebay_result = bp.process_web_form_request(text)
    if ebay_result:
        output = table.format(line=1,
                              id=ebay_result['insert']['product']['ITEM_ID'],
                              title=ebay_result['insert']['product']['TITLE'],
                              price=ebay_result['insert']['selling_status']['CURRENT_PRICE_VALUE'],
                              currency=ebay_result['insert']['selling_status']['CURRENT_PRICE_CURRENCY_ID'],
                              payment_method=ebay_result['insert']['product']['PAYMENT_METHOD'],
                              results="Table above contains first search result. {} items found in total.".format(
                                  ebay_result['items_received']))
    else:
        multiple = "s" if "," in text else ""
        output = "Sorry, no results for keyword{}: {}".format(multiple, text)

print('Content-type: text/html\n')

print(pattern.format(posts=output))