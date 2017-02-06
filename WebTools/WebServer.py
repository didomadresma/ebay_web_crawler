#!/usr/bin/env python3
__author__ = 'papermakkusu'


from http.server import HTTPServer, CGIHTTPRequestHandler
server_address = ("localhost", 8000)
httpd = HTTPServer(server_address, CGIHTTPRequestHandler)
httpd.serve_forever()
